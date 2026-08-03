import logging
import os
import random
import sys
import time
from typing import List
from config_manager import ConfigManager
from snowpipe_streaming_manager import SnowpipeStreamingManager
from snowflake.ingest.streaming.streaming_ingest_error import StreamingIngestError
from reconciliation_manager import ReconciliationManager
from data_generator import DataGenerator
from models import Order, OrderItem

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class AutomatedIntelligenceStreaming:
    def __init__(
        self, config: ConfigManager, streaming_manager: SnowpipeStreamingManager
    ):
        self.config = config
        self.streaming_manager = streaming_manager

    def generate_and_stream_orders(self, num_orders: int) -> None:
        logger.info(f"Starting to generate and stream {num_orders} orders")
        
        max_customer_id = self.streaming_manager.get_max_customer_id()
        if max_customer_id == 0:
            logger.error(
                "No customers found in database. Please run generate_customers() "
                "stored procedure first to create customers."
            )
            raise ValueError("No customers available for order generation")
        
        logger.info(f"Will generate orders for customer IDs in range 1-{max_customer_id}")
        
        batch_size = self.config.get_int_property("orders.batch.size", 10000)
        logger.info(f"Using batch size: {batch_size} orders per insertRows call")
        
        processed_orders = 0
        max_retries = 3
        
        while processed_orders < num_orders:
            remaining_orders = num_orders - processed_orders
            current_batch_size = min(batch_size, remaining_orders)
            
            # Generate data once for this batch
            order_batch: List[Order] = []
            all_order_items: List[OrderItem] = []
            
            for i in range(current_batch_size):
                customer_id = DataGenerator.random_customer_id(max_customer_id)
                customer_segment = self.streaming_manager.get_customer_segment(customer_id)
                order = DataGenerator.generate_order(customer_id, customer_segment)
                order_batch.append(order)
                
                item_count = DataGenerator.random_item_count(customer_segment)
                order_items = DataGenerator.generate_order_items(
                    order.order_id, customer_segment, item_count
                )
                all_order_items.extend(order_items)
            
            # Insert orders and items separately with individual retry logic
            # This prevents duplicate orders when items fail but orders succeed
            orders_inserted = False
            items_inserted = False
            
            # Step 1: Insert orders with retry (exponential backoff + jitter)
            for retry_count in range(max_retries + 1):
                try:
                    self.streaming_manager.insert_orders(order_batch)
                    orders_inserted = True
                    break
                except StreamingIngestError as e:
                    if retry_count >= max_retries:
                        logger.error(
                            f"Failed to insert orders after {max_retries + 1} attempts: {e}",
                            exc_info=True
                        )
                        raise
                    delay = min(2 ** retry_count, 16)
                    jitter = random.uniform(0, delay * 0.25)
                    logger.warning(
                        f"Orders insert failed (attempt {retry_count + 1}/{max_retries + 1}), "
                        f"retrying in {delay + jitter:.1f}s: {e}"
                    )
                    time.sleep(delay + jitter)
            
            # Step 2: Brief pause before inserting items
            time.sleep(0.1)
            
            # Step 3: Insert order_items with retry (exponential backoff + jitter)
            for retry_count in range(max_retries + 1):
                try:
                    self.streaming_manager.insert_order_items(all_order_items)
                    items_inserted = True
                    break
                except StreamingIngestError as e:
                    if retry_count >= max_retries:
                        logger.error(
                            f"Failed to insert order_items after {max_retries + 1} attempts: {e}",
                            exc_info=True
                        )
                        # Items failed but orders succeeded - reconciliation will clean this up
                        logger.warning(
                            f"ATOMICITY VIOLATION: {len(order_batch)} orders inserted but "
                            f"{len(all_order_items)} order_items failed. Reconciliation will clean up."
                        )
                        raise
                    delay = min(2 ** retry_count, 16)
                    jitter = random.uniform(0, delay * 0.25)
                    logger.warning(
                        f"Order_items insert failed (attempt {retry_count + 1}/{max_retries + 1}), "
                        f"retrying in {delay + jitter:.1f}s: {e}"
                    )
                    time.sleep(delay + jitter)
            
            # Both succeeded
            if orders_inserted and items_inserted:
                processed_orders += current_batch_size
                logger.info(
                    f"Progress: {processed_orders}/{num_orders} orders streamed "
                    f"({len(all_order_items)} order items)"
                )
        
        logger.info(f"Successfully streamed {num_orders} orders")
        self._print_offset_status()

    def _print_offset_status(self) -> None:
        logger.info("=== Offset Token Status ===")
        logger.info(f"Orders: {self.streaming_manager.get_latest_order_offset()}")
        logger.info(
            f"Order Items: {self.streaming_manager.get_latest_order_item_offset()}"
        )


def main():
    logger.info("Starting Automated Intelligence Snowpipe Streaming")
    
    config = None
    streaming_manager = None
    
    try:
        _root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
        config_file = os.path.join(_root, "config_default.properties")
        profile_file = os.path.join(_root, "profile.json")
        num_orders = None

        if len(sys.argv) > 1:
            num_orders = int(sys.argv[1])
        if len(sys.argv) > 2:
            config_file = sys.argv[2]
        if len(sys.argv) > 3:
            profile_file = sys.argv[3]
        
        config = ConfigManager(config_file, profile_file)
        streaming_manager = SnowpipeStreamingManager(config)
        
        app = AutomatedIntelligenceStreaming(config, streaming_manager)
        
        if num_orders is None:
            num_orders = config.get_int_property("num.orders.per.batch", 100)
        
        app.generate_and_stream_orders(num_orders)
        
        logger.info("Waiting for all channel data to flush to Snowflake...")
        flushed = streaming_manager.wait_for_flush(timeout_seconds=120)
        if not flushed:
            logger.warning(
                "Channel flush timed out after 120s. "
                "Reconciliation may report false orphans."
            )

        # wait_for_flush confirms the SDK-side buffer is drained, but Snowflake's
        # server-side commit can take up to max.client.lag seconds after that.
        # Running reconciliation before the server commit completes would delete
        # valid records as false orphans (orders with no items yet, or vice versa).
        server_lag = config.get_int_property("max.client.lag", 60)
        post_flush_wait = server_lag + 10
        logger.info(
            f"Waiting {post_flush_wait}s for server-side commit "
            f"(max.client.lag={server_lag}s + 10s buffer)..."
        )
        time.sleep(post_flush_wait)
        
        # Run reconciliation to clean up any orphaned records
        logger.info("\n" + "="*60)
        logger.info("Starting post-ingestion reconciliation...")
        logger.info("="*60)
        
        try:
            reconciliation_manager = ReconciliationManager(config)
            reconciliation_stats = reconciliation_manager.reconcile_and_cleanup()
            
            # Report if any inconsistencies were found
            if reconciliation_stats["orphaned_orders_found"] > 0 or reconciliation_stats["orphaned_items_found"] > 0:
                logger.warning(
                    f"⚠️  Data inconsistencies detected and cleaned: "
                    f"{reconciliation_stats['orphaned_orders_deleted']:,} orphaned orders, "
                    f"{reconciliation_stats['orphaned_items_deleted']:,} orphaned order_items"
                )
            else:
                logger.info("✅ No data inconsistencies found - ingestion was atomic")
                
        except Exception as e:
            logger.error(f"Reconciliation failed: {e}", exc_info=True)
            logger.warning("⚠️  Reconciliation failed but ingestion completed. Manual cleanup may be needed.")
        
        logger.info("="*60 + "\n")
        
        logger.info("Application completed successfully")
        
    except Exception as e:
        logger.error("Application error", exc_info=True)
        sys.exit(1)
    finally:
        if streaming_manager is not None:
            streaming_manager.close()


if __name__ == "__main__":
    main()
