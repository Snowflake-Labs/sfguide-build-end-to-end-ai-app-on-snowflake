# Snowpipe Streaming for Automated Intelligence - Python SDK

Python implementation of real-time data ingestion using the Snowpipe Streaming Python SDK for the Automated Intelligence demo.

## Overview

This application streams synthetic e-commerce data (customers, orders, and order items) directly into Snowflake tables using the high-performance Snowpipe Streaming Python SDK. It provides:

- **Real-time ingestion** with low end-to-end latency
- **Exactly-once delivery** using offset tokens
- **Parallel streaming** with multiple concurrent instances
- **Resumable ingestion** from last committed offset
- **Same business logic** as the Java implementation
- **Segment-based order generation**: Orders vary by customer segment (Premium, Standard, Basic)
  - Premium: $500-$3000 orders, 10% discount rate (5-10% off), 3-8 items/order
  - Standard: $100-$800 orders, 40% discount rate (5-20% off), 2-5 items/order
  - Basic: $20-$300 orders, 50% discount rate (10-30% off), 1-3 items/order

**Note:** *Performance varies by Snowflake account configuration, region, and network conditions.*

## Architecture

### Tables
- `DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMERS`
- `DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS`
- `DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDER_ITEMS`

### Channels (2 channels - customers not streamed)
1. **orders_channel** → `ORDERS` table
2. **order_items_channel** → `ORDER_ITEMS` table

### Offset Tokens
- Orders: `order_<order_id>`
- Order Items: `item_<order_item_id>`

## Prerequisites

- **Python**: 3.9 or later
- **Snowflake Account**: With appropriate database, schema, and tables
- **Authentication**: Key-pair authentication (RSA private key)
- **Permissions**: 
  - INSERT on target tables
  - USAGE on database/schema
  - OPERATE on PIPE objects

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

**SDK Version:** 1.1.2+ (Jan 2026) - includes Azure/GCP GA support and bug fixes.

### 2. Grant Streaming Privileges

The `ORDERS_STAGING-STREAMING` and `ORDER_ITEMS_STAGING-STREAMING` pipes are auto-created by Snowflake on first connect using the reserved `<TABLE_NAME>-STREAMING` naming convention — do **not** create them manually. You only need to grant the required privileges.

Run the following SQL as `ACCOUNTADMIN`:

```sql
USE ROLE ACCOUNTADMIN;

GRANT USAGE  ON SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.STAGING                    TO ROLE AUTOMATED_INTELLIGENCE_ADMIN;
GRANT INSERT ON TABLE  DASH_AUTOMATED_INTELLIGENCE_DB.STAGING.ORDERS_STAGING     TO ROLE AUTOMATED_INTELLIGENCE_ADMIN;
GRANT INSERT ON TABLE  DASH_AUTOMATED_INTELLIGENCE_DB.STAGING.ORDER_ITEMS_STAGING TO ROLE AUTOMATED_INTELLIGENCE_ADMIN;
GRANT CREATE PIPE ON SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.STAGING                TO ROLE AUTOMATED_INTELLIGENCE_ADMIN;
```

### 3. Generate RSA Key Pair for Authentication

```bash
cd snowpipe-streaming-python

# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### 4. Assign Public Key to Snowflake User

```sql
ALTER USER YOUR_USER SET RSA_PUBLIC_KEY='<public_key_content>';
```

Replace `<public_key_content>` with the content of `rsa_key.pub` (without BEGIN/END headers).

### 5. Configure Application

Copy the template and fill in your credentials:

```bash
cp profile.json.template profile.json
```

Edit `profile.json`:
```json
{
  "account": "YOUR_ACCOUNT",
  "user": "YOUR_USER",
  "url": "https://YOUR_ACCOUNT.snowflakecomputing.com",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
  "database": "DASH_AUTOMATED_INTELLIGENCE_DB",
  "schema": "RAW",
  "warehouse": "AUTOMATED_INTELLIGENCE_WH",
  "role": "AUTOMATED_INTELLIGENCE_ADMIN"
}
```

**Important Configuration Notes:**
- **account**: Use hyphens, not underscores (e.g., `sfsenorthamerica-gen-ai-hol`, NOT `sfsenorthamerica-gen_ai_hol`)
- **role**: Must specify a valid role (SDK does not default to user's default role)
- **private_key**: Paste the **entire private key** from `rsa_key.p8` including headers
- **schema**: Use `RAW` for production, `STAGING` for staging environment

## Usage

### Single-Instance Streaming

Stream orders using a single Python process:

```bash
cd src
python automated_intelligence_streaming.py [num_orders]
```

Examples:
```bash
# Stream 100 orders (default)
python automated_intelligence_streaming.py

# Stream 10,000 orders
python automated_intelligence_streaming.py 10000

# Use custom config and profile files
python automated_intelligence_streaming.py 1000 config_staging.properties profile_staging.json

# Defaults: config_default.properties and profile.json
```

### Parallel Streaming (High Throughput)

Scale horizontally with multiple concurrent instances:

```bash
cd src
python parallel_streaming_orchestrator.py <total_orders> <num_instances>
```

Examples:
```bash
# Stream 1M orders using 5 parallel instances
python parallel_streaming_orchestrator.py 1000000 5

# Stream 10M orders using 10 parallel instances
python parallel_streaming_orchestrator.py 10000000 10
```

**How it works:**
- Partitions the customer ID range across instances
- Each instance uses separate channels with unique names
- Prevents ID collisions using offset token tracking
- Runs all instances concurrently in thread pool

## Configuration

Edit `config_default.properties` to tune performance:

```properties
# Batch size: Orders per append_rows() call
orders.batch.size=10000

# Max client lag: Higher = better compression, lower = lower latency
max.client.lag=60

# Default orders to generate
num.orders.per.batch=100
```

### Understanding Data Flush Behavior

Data doesn't appear in Snowflake tables immediately after calling `insertRows()`. Snowpipe Streaming uses intelligent buffering for optimal performance:

**Key Configuration:**
- **`max.client.lag=60`**: Buffers data for up to **60 seconds** before flushing to tables
  - Optimizes query performance through better file partitioning
  - Recommended default by Snowflake for production workloads

**Flush Triggers:**
Data is flushed to Snowflake when **any** of these conditions are met:
1. **Buffer size threshold** (~16 MB compressed data)
2. **Time threshold** (60 seconds elapsed since first record)
3. **Channel closed** (explicit close or application shutdown)

**Latency Trade-offs:**
- **Lower lag** (e.g., `max.client.lag=5`):
  - ✅ Faster data visibility (~5 seconds)
  - ❌ More frequent flushes = smaller files = slower downstream queries
  - Use for: Demos, real-time dashboards

- **Higher lag** (e.g., `max.client.lag=60`):
  - ✅ Better file sizes = faster queries
  - ✅ Better compression and throughput
  - ❌ Slower data visibility (~60 seconds)
  - Use for: Production workloads

**Example:** With `max.client.lag=60`, expect data to appear in tables within **~60 seconds** after ingestion starts. This is normal and optimal for production!

## Project Structure

```
snowpipe-streaming-python/
├── src/
│   ├── models.py                              # Customer, Order, OrderItem data models
│   ├── data_generator.py                      # Business logic for synthetic data
│   ├── config_manager.py                      # Configuration loader
│   ├── id_tracker.py                          # Offset token parsing and ID generation
│   ├── snowpipe_streaming_manager.py          # Snowpipe SDK wrapper
│   ├── automated_intelligence_streaming.py    # Single-instance application
│   └── parallel_streaming_orchestrator.py     # Multi-instance orchestrator
├── config_default.properties                  # Default configuration (RAW schema)
├── config_staging.properties                  # Staging environment configuration
├── profile.json.template                      # Snowflake credentials template
├── requirements.txt                           # Python dependencies
└── README.md                                  # This file
```

## Key Features

### Exactly-Once Delivery
- Uses offset tokens to track ingestion progress
- Resumes from last committed position on restart
- Prevents duplicate data ingestion

### ID Generation Strategy
- Parses last committed offset token on startup
- Generates sequential IDs from last position
- Thread-safe ID allocation with locks

### Error Handling
- Detailed logging at INFO and DEBUG levels
- Automatic retries for transient failures
- Graceful shutdown with channel/client cleanup

### Performance Optimization
- Batch inserts with `append_rows()` (vs single-row `append_row()`)
- Configurable batch sizes (default: 10,000 orders)
- Parallel streaming with customer ID partitioning

## Comparison with Java Implementation

| Feature | Java SDK | Python SDK |
|---------|----------|------------|
| **Package** | `snowflake-ingest-java` | `snowpipe-streaming` |
| **Performance** | Native JVM | Rust-backed (high performance) |
| **API Style** | `SnowflakeStreamingIngestClient` | `StreamingIngestClient` |
| **Channel Opening** | `client.openChannel()` | `client.open_channel()` |
| **Batch Insert** | `channel.appendRows()` | `channel.append_rows()` |
| **Offset Tracking** | `getLatestCommittedOffsetToken()` | `get_latest_committed_offset_token()` |
| **Business Logic** | Identical | Identical |
| **Data Generation** | Identical | Identical |
| **Scaling** | Multi-process | Multi-threaded |

## Monitoring

Check ingestion progress:

```sql
-- View channel status
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_CHANNEL_STATUS('DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS_PIPE'));

-- Check row counts
SELECT COUNT(*) FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS;
SELECT COUNT(*) FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDER_ITEMS;
```

## Troubleshooting

### JWT Authentication Error (Error 390144)
```
Error: HTTP 401, error_code=390144, message=JWT token is invalid
```
**Solution**: This is a known bug in Snowpipe Streaming SDK v1.1.0. Downgrade to v1.0.2:
```bash
pip install snowpipe-streaming==1.0.2
```

### Authentication Errors
- Verify private key format includes headers: `-----BEGIN PRIVATE KEY-----`
- Ensure public key is assigned to user in Snowflake
- Check user has necessary permissions
- **Verify account identifier uses hyphens** (e.g., `gen-ai-hol`, NOT `gen_ai_hol`)
- **Ensure role field is present** in profile.json

### No Data Appearing
- Verify PIPE objects exist: `SHOW PIPES IN SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.RAW;`
- Check for errors: `SELECT * FROM TABLE(VALIDATE_PIPE_LOAD('ORDERS_PIPE', ...));`
- Wait briefly for data to be visible (ingestion latency varies)
- Check schema setting in profile.json matches your target tables

### Performance Issues
- Increase `orders.batch.size` in config.properties
- Use parallel streaming for large volumes
- Monitor warehouse size and scaling

### Orphaned Records
If streaming fails mid-batch, you may have orphaned orders (orders without order_items):
```bash
# Run reconciliation to clean up
cd src
python -c "
import sys
sys.path.insert(0, '.')
from config_manager import ConfigManager
from reconciliation_manager import ReconciliationManager
config = ConfigManager('config.properties', 'profile.json')
ReconciliationManager(config).reconcile_and_cleanup()
"
```

## Architecture Notes

### High-Performance Pipe-Based Architecture (GA Sep 2025)

This codebase uses the **high-performance Snowpipe Streaming SDK** (`snowpipe-streaming` on PyPI),
which replaced the classic `snowflake-ingest` package. The new SDK has a shared Rust core
available in both Java and Python.

Key characteristics:

- **Different package**: `pip install snowpipe-streaming` (not `snowflake-ingest`)
- **`StreamingIngestClient` takes a `pipe_name` parameter** that routes data through optimized Rust ingestion paths. See `snowpipe_streaming_manager.py:36-50` where separate clients are created per pipe (`ORDERS_PIPE`, `ORDER_ITEMS_PIPE`).
- **`append_rows(rows, start_offset, end_offset)`** for batch inserts with offset token ranges, enabling exactly-once delivery. See `snowpipe_streaming_manager.py:200`.
- **`append_row(row, offset_token)`** for single-row inserts. See `snowpipe_streaming_manager.py:137`.
- **ReceiverSaturated / HTTP 429 backpressure handling** with exponential backoff + jitter (initial 1s, max 30s, 5 retries). See `_insert_with_backpressure_retry()` at `snowpipe_streaming_manager.py:172-235`.
- **Automatic micro-batching** by the Rust core — the SDK accumulates rows client-side and flushes based on `max.client.lag` (time) or buffer size (~16 MB compressed).
- **Per-GB billing** instead of compute + client count billing from the classic SDK.
- **Server-side schema enforcement** through the PIPE definition, not client-side validation.

**Performance**: Sub-second end-to-end latency (with low `max.client.lag`), millions of rows/sec per client, governed by network bandwidth and Snowflake receiver capacity.

### Migration from Classic SDK (`snowflake-ingest`)

If upgrading from the classic `snowflake-ingest` package:

| Area | Classic (`snowflake-ingest`) | High-Performance (`snowpipe-streaming`) |
|------|-----|------|
| Package | `pip install snowflake-ingest` | `pip install snowpipe-streaming` |
| Entry point | Data ingested directly into tables | Data ingested through PIPE objects |
| Client mapping | One client, many tables | One client per pipe |
| API | `insert_row` / `insert_rows` | `append_row` / `append_rows` |
| Backpressure | Blocks thread (sleep) | Returns error (caller retries) |
| Billing | Compute + client count | Flat per-GB ingested |

Steps:

1. **Create pipe objects** in Snowflake:
   ```sql
   CREATE PIPE my_pipe AS COPY INTO my_table
       FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
       MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
   ```

2. **Swap the pip package**:
   ```bash
   pip uninstall snowflake-ingest
   pip install snowpipe-streaming
   ```

3. **Pass `pipe_name` to `StreamingIngestClient`**:
   ```python
   # Old (classic, no pipe)
   client = StreamingIngestClient(
       client_name="MY_CLIENT",
       db_name="MY_DB",
       schema_name="MY_SCHEMA",
       properties=props,
   )

   # New (high-performance, pipe-based)
   client = StreamingIngestClient(
       client_name="MY_CLIENT",
       db_name="MY_DB",
       schema_name="MY_SCHEMA",
       pipe_name="MY_PIPE",       # <-- add this
       properties=props,
   )
   ```

4. **Replace insert calls** with offset-tracked variants:
   ```python
   # Old
   channel.insert_row(row)

   # New — single row
   channel.append_row(row, offset_token)

   # New — batch (preferred)
   channel.append_rows(rows, start_offset, end_offset)
   ```

5. **Add backpressure retry logic** for `ReceiverSaturated` errors. The pipe-based architecture applies backpressure when the receiver is saturated — callers must handle `StreamingIngestError` containing `ReceiverSaturated` or HTTP 429 and retry with exponential backoff. See `_insert_with_backpressure_retry()` in this repo for a reference implementation.

6. **Grant OPERATE on pipes** to your streaming role:
   ```sql
   GRANT OPERATE ON PIPE my_pipe TO ROLE my_role;
   ```

For the full migration guide, see the [Snowflake docs](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-migration).

## License

Apache License 2.0

## Generated with Snowflake CoCo

This implementation was created using Snowflake Snowflake CoCo to replicate the Java SDK implementation in Python.
