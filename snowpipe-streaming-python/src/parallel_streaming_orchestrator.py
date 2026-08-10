import logging
import os
import sys
import time
from typing import List
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from config_manager import ConfigManager
from snowpipe_streaming_manager import SnowpipeStreamingManager
from reconciliation_manager import ReconciliationManager
from data_generator import DataGenerator
from models import Order, OrderItem

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class ParallelStreamingOrchestrator:
    @staticmethod
    def main(total_orders: int, num_instances: int, config_file: str = "config.properties", profile_file: str = "profile.json"):
        logger.info("=== Parallel Streaming Orchestrator ===")
        logger.info(f"Total orders to generate: {total_orders}")
        logger.info(f"Number of parallel instances: {num_instances}")
        logger.info(f"Using config: {config_file}")
        
        config = None
        
        try:
            config = ConfigManager(config_file, profile_file)
            max_customer_id = ParallelStreamingOrchestrator._get_max_customer_id(config)
            
            logger.info(f"Total customers available: {max_customer_id}")
            
            orders_per_instance = total_orders // num_instances
            customer_range_size = max_customer_id // num_instances
            
            with ThreadPoolExecutor(max_workers=num_instances) as executor:
                futures: List[Future] = []
                
                for i in range(num_instances):
                    orders_for_this_instance = (
                        total_orders - (orders_per_instance * i)
                        if i == num_instances - 1
                        else orders_per_instance
                    )
                    
                    customer_id_start = (i * customer_range_size) + 1
                    customer_id_end = (
                        max_customer_id
                        if i == num_instances - 1
                        else (i + 1) * customer_range_size
                    )
                    
                    logger.info(
                        f"Instance {i}: {orders_for_this_instance} orders, "
                        f"customer IDs {customer_id_start}-{customer_id_end}"
                    )
                    
                    future = executor.submit(
                        ParallelStreamingOrchestrator._run_streaming_instance,
                        i,
                        orders_for_this_instance,
                        customer_id_start,
                        customer_id_end,
                        config,
                    )
                    futures.append(future)
                
                logger.info(
                    f"All {num_instances} instances submitted. Waiting for completion..."
                )
                
                total_orders_generated = 0
                successful_instances = 0
                failed_instances = 0
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result["success"]:
                            total_orders_generated += result["orders_generated"]
                            successful_instances += 1
                            logger.info(
                                f"Instance {result['instance_id']} completed: {result['orders_generated']} orders "
                                f"in {result['duration_ms']} ms"
                            )
                        else:
                            failed_instances += 1
                            logger.error(
                                f"Instance {result['instance_id']} failed with {result['orders_generated']} orders "
                                f"generated before failure"
                            )
                    except Exception as e:
                        failed_instances += 1
                        logger.error(f"Instance failed with exception: {e}", exc_info=True)
                
                logger.info("=== Parallel Streaming Completed ===")
                logger.info(
                    f"Successful instances: {successful_instances}/{num_instances}"
                )
                logger.info(f"Failed instances: {failed_instances}")
                logger.info(f"Total orders generated: {total_orders_generated}")
                
                # Always run reconciliation to clean up any orphaned records
                # (Even "failed" instances may have inserted partial data before failing)
                logger.info("\n" + "="*60)
                logger.info("Starting post-ingestion reconciliation...")
                logger.info("="*60)
                
                try:
                    reconciliation_manager = ReconciliationManager(config)
                    reconciliation_stats = reconciliation_manager.reconcile_and_cleanup()
                    
                    # Report if any inconsistencies were found
                    if (reconciliation_stats["orphaned_orders_found"] > 0 or 
                        reconciliation_stats["orphaned_items_found"] > 0 or
                        reconciliation_stats["duplicate_orders_found"] > 0):
                        logger.warning(
                            f"⚠️  Data inconsistencies detected and cleaned: "
                            f"{reconciliation_stats['orphaned_orders_deleted']:,} orphaned orders, "
                            f"{reconciliation_stats['orphaned_items_deleted']:,} orphaned order_items, "
                            f"{reconciliation_stats['duplicate_orders_deleted']:,} duplicate orders"
                        )
                    else:
                        logger.info("✅ No data inconsistencies found - ingestion was atomic")
                        
                except Exception as e:
                    logger.error(f"Reconciliation failed: {e}", exc_info=True)
                    logger.warning("⚠️  Reconciliation failed but ingestion completed. Manual cleanup may be needed.")
                
                logger.info("="*60 + "\n")
                
                if failed_instances > 0:
                    sys.exit(1)
                    
        except Exception as e:
            logger.error("Orchestrator error", exc_info=True)
            sys.exit(1)

    @staticmethod
    def _run_streaming_instance(
        instance_id: int,
        num_orders: int,
        customer_id_start: int,
        customer_id_end: int,
        config: ConfigManager,
    ) -> dict:
        logger.info(
            f"Instance {instance_id} starting: {num_orders} orders, "
            f"customers {customer_id_start}-{customer_id_end}"
        )
        
        start_time = time.time()
        streaming_manager = None
        orders_generated = 0
        
        try:
            streaming_manager = SnowpipeStreamingManager(config, instance_id)
            app = PartitionedStreamingApp(
                config, streaming_manager, customer_id_start, customer_id_end
            )
            
            orders_generated = app.generate_and_stream_orders(num_orders)
            
            time.sleep(2)
            
            duration_ms = int((time.time() - start_time) * 1000)
            return {
                "instance_id": instance_id,
                "orders_generated": orders_generated,
                "duration_ms": duration_ms,
                "success": True,
            }
            
        except Exception as e:
            logger.error(f"Instance {instance_id} error: {e}", exc_info=True)
            duration_ms = int((time.time() - start_time) * 1000)
            return {
                "instance_id": instance_id,
                "orders_generated": orders_generated,
                "duration_ms": duration_ms,
                "success": False,
            }
        finally:
            if streaming_manager is not None:
                streaming_manager.close()

    @staticmethod
    def _get_max_customer_id(config: ConfigManager) -> int:
        temp_manager = None
        try:
            temp_manager = SnowpipeStreamingManager(config, -1)
            return temp_manager.get_max_customer_id()
        finally:
            if temp_manager is not None:
                temp_manager.close()


class PartitionedStreamingApp:
    def __init__(
        self,
        config: ConfigManager,
        streaming_manager: SnowpipeStreamingManager,
        customer_id_start: int,
        customer_id_end: int,
    ):
        self.config = config
        self.streaming_manager = streaming_manager
        self.customer_id_start = customer_id_start
        self.customer_id_end = customer_id_end

    def generate_and_stream_orders(self, num_orders: int) -> int:
        logger.info(
            f"Starting partitioned streaming: {num_orders} orders, "
            f"customer range {self.customer_id_start}-{self.customer_id_end}"
        )
        
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
                customer_id = DataGenerator.random_customer_id_in_range(
                    self.customer_id_start, self.customer_id_end
                )
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
            
            # Step 1: Insert orders with retry
            for retry_count in range(max_retries + 1):
                try:
                    self.streaming_manager.insert_orders(order_batch)
                    orders_inserted = True
                    break
                except Exception as e:
                    if retry_count >= max_retries:
                        logger.error(
                            f"Failed to insert orders after {max_retries + 1} attempts: {e}",
                            exc_info=True
                        )
                        raise
                    logger.warning(
                        f"Orders insert failed (attempt {retry_count + 1}/{max_retries + 1}), retrying: {e}"
                    )
                    time.sleep(1 * (retry_count + 1))
            
            # Step 2: Brief pause before inserting items
            time.sleep(0.1)
            
            # Step 3: Insert order_items with retry
            for retry_count in range(max_retries + 1):
                try:
                    self.streaming_manager.insert_order_items(all_order_items)
                    items_inserted = True
                    break
                except Exception as e:
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
                    logger.warning(
                        f"Order_items insert failed (attempt {retry_count + 1}/{max_retries + 1}), retrying: {e}"
                    )
                    time.sleep(1 * (retry_count + 1))
            
            # Both succeeded
            if orders_inserted and items_inserted:
                processed_orders += current_batch_size
                logger.info(
                    f"Progress: {processed_orders}/{num_orders} orders streamed "
                    f"({len(all_order_items)} order items)"
                )
        
        logger.info(
            f"Successfully streamed {num_orders} orders "
            f"(customer range: {self.customer_id_start}-{self.customer_id_end})"
        )
        
        return processed_orders


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Usage: python parallel_streaming_orchestrator.py <total_orders> <num_parallel_instances> [config_file] [profile_file]"
        )
        print("Example: python parallel_streaming_orchestrator.py 1000000 5")
        print("Example: python parallel_streaming_orchestrator.py 100000 5 config_staging.properties profile_staging.json")
        sys.exit(1)
    
    total_orders = int(sys.argv[1])
    num_instances = int(sys.argv[2])
    _root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    config_file = sys.argv[3] if len(sys.argv) > 3 else os.path.join(_root, "config_default.properties")
    profile_file = sys.argv[4] if len(sys.argv) > 4 else os.path.join(_root, "profile.json")
    
    ParallelStreamingOrchestrator.main(total_orders, num_instances, config_file, profile_file)
