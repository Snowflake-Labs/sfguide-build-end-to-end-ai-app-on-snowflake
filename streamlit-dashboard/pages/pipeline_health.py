import streamlit as st
import pandas as pd
from shared import get_session, show_header, format_number

show_header()
st.subheader("🏥 Pipeline Health")
st.divider()
st.markdown("#### 🔄 Dynamic Tables Status")

session = get_session()

# Dynamic Tables info accordion
with st.expander("ℹ️ About Dynamic Tables", expanded=False):
    st.markdown("""
    **What are Dynamic Tables?**
    
    Dynamic Tables are declarative, materialized views that automatically refresh and maintain themselves based on dependencies. 
    They simplify data pipeline management by eliminating the need for manual orchestration.
    
    **Purpose:**
    - Automatically transform and refresh data as upstream sources change
    - Define transformations using simple SQL SELECT statements
    - Snowflake manages the refresh schedule and dependencies
    
    **Benefits:**
    - 🔄 **Automated Refresh**: Refreshes automatically based on target lag settings
    - 🔗 **Dependency Tracking**: Automatically detects upstream table changes
    - ⚡ **Incremental Processing**: Only processes changed data for efficiency
    - 🛠️ **Simplified Orchestration**: No need for external schedulers or task management
    - 📊 **Pipeline Observability**: Built-in monitoring of refresh status and lag
    """)

# First check if there's any data in source tables
source_check_query = """
SELECT COUNT(*) as order_count FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS
"""
source_result = session.sql(source_check_query).collect()
source_has_data = source_result[0]['ORDER_COUNT'] > 0

if not source_has_data:
    st.warning("⚠️ No data available in source tables. Stream data first using Snowpipe Streaming to populate Dynamic Tables.")
else:
    dt_status_query = """
    SHOW DYNAMIC TABLES IN DASH_AUTOMATED_INTELLIGENCE_DB.DYNAMIC_TABLES
    """
    
    try:
        dt_rows = session.sql(dt_status_query).collect()
        
        if dt_rows:
            dt_df = pd.DataFrame([row.as_dict() for row in dt_rows])
            
            display_columns = ['name', 'scheduling_state', 'refresh_mode', 'target_lag', 'warehouse']
            available_columns = [col for col in display_columns if col in dt_df.columns]
            st.dataframe(dt_df[available_columns], use_container_width=True, hide_index=True)
            
            all_active = all(dt_df['scheduling_state'] == 'ACTIVE')
            if all_active:
                st.success("✅ All Dynamic Tables are ACTIVE")
            else:
                st.warning("⚠️ Some Dynamic Tables are not ACTIVE")
        else:
            st.info("No Dynamic Tables found")
    except Exception as e:
        st.error(f"Error querying Dynamic Tables: {str(e)}")

# Dynamic Tables "In This Demo" section
with st.expander("📋 Dynamic Tables in This Demo", expanded=True):
    st.markdown("""
    Our pipeline uses 5 Dynamic Tables to transform raw streaming data into analytics-ready datasets:
    
    1. **ENRICHED_ORDERS** - Enriches raw orders with calculated fields like order totals and status flags
    2. **ENRICHED_ORDER_ITEMS** - Adds product category information and calculates line item totals
    3. **FACT_ORDERS** - Creates a clean, denormalized fact table joining orders with customer and product data
    4. **DAILY_BUSINESS_METRICS** - Aggregates daily revenue, order counts, and customer metrics
    5. **PRODUCT_PERFORMANCE_METRICS** - Summarizes product sales performance across categories
    
    **Data Flow:**
    - Raw data arrives via Snowpipe Streaming → RAW schema
    - Dynamic Tables automatically detect new data and refresh
    - Each Dynamic Table builds on upstream tables, creating a layered transformation pipeline
    - Target lag of 1 minute ensures near real-time analytics
    - All refreshes happen incrementally without manual intervention
    """)

st.divider()

# Check Interactive Tables status
st.markdown("#### ⚡ Interactive Tables Status")

# Interactive Tables info accordion
with st.expander("ℹ️ About Interactive Tables", expanded=False):
    st.markdown("""
    **What are Interactive Tables?**
    
    Interactive Tables are a new table type in Snowflake optimized for low-latency, high-concurrency queries with automatic 
    indexing and caching for sub-second response times on small to medium datasets.
    
    **Purpose:**
    - Enable real-time, interactive analytics on frequently queried data
    - Power dashboards and applications requiring fast response times
    - Eliminate the need for manual index management
    
    **Benefits:**
    - ⚡ **Sub-Second Queries**: Optimized for low-latency, interactive workloads
    - 🎯 **Automatic Indexing**: Snowflake automatically creates and maintains optimal indexes
    - 🔄 **High Concurrency**: Handle many concurrent users efficiently
    - 💰 **Cost-Effective**: No need for large warehouses for simple lookups
    - 🚀 **Zero Tuning**: No manual index creation or maintenance required
    
    **Best Use Cases:**
    - Point lookups and key-based queries
    - Dashboard queries requiring fast response times
    - Application backends with many concurrent users
    - Small to medium-sized datasets (millions of rows)
    """)

col1, col2, col3 = st.columns(3)

interactive_metrics_query = """
SELECT 
    ROUND(AVG(total_orders), 2) as avg_orders_per_customer,
    ROUND(SUM(total_spent), 2) as total_revenue,
    COUNT(DISTINCT customer_id) as total_customers
FROM DASH_AUTOMATED_INTELLIGENCE_DB.INTERACTIVE.CUSTOMER_ORDER_ANALYTICS
"""

try:
    metrics_df = session.sql(interactive_metrics_query).to_pandas()
    
    if not metrics_df.empty:
        avg_orders = metrics_df['AVG_ORDERS_PER_CUSTOMER'].iloc[0]
        total_revenue = metrics_df['TOTAL_REVENUE'].iloc[0]
        total_customers = metrics_df['TOTAL_CUSTOMERS'].iloc[0]
        
        # Check if there's actual data (not null or zero customers)
        if pd.notna(total_customers) and total_customers > 0:
            col1.metric("Avg Orders per Customer", f"{avg_orders:,.2f}" if pd.notna(avg_orders) else "0")
            col2.metric("Total Revenue", f"${format_number(total_revenue)}" if pd.notna(total_revenue) else "$0.00")
            col3.metric("Total Customers", format_number(total_customers, include_decimals=False))
            
            st.success("✅ Interactive Tables are populated and up-to-date")
        else:
            col1.metric("Avg Orders per Customer", "0")
            col2.metric("Total Revenue", "$0.00")
            col3.metric("Total Customers", "0")
            
            st.info("📋 **Interactive Tables not yet populated**\n\nInteractive Tables will populate automatically once data flows through Dynamic Tables.")
    else:
        st.warning("⚠️ Interactive Tables may be empty")
except Exception as e:
    st.error(f"Error querying Interactive Tables: {str(e)}")

# Interactive Tables "In This Demo" section
with st.expander("📋 Interactive Tables in This Demo", expanded=True):
    st.markdown("""
    We use Interactive Tables to power high-performance analytics in the INTERACTIVE schema:
    
    **CUSTOMER_ORDER_ANALYTICS** - Pre-aggregated customer metrics including:
    - Total orders per customer
    - Total spend per customer
    - Average order value
    - First and last order dates
    - Customer lifetime value calculations
    
    **Why Interactive Tables Here?**
    - **Dashboard Performance**: Powers the Summary page with instant load times
    - **Point Lookups**: Customer-level queries return in milliseconds
    - **High Concurrency**: Multiple users can query simultaneously without warehouse scaling
    - **Cost Efficiency**: Eliminates need for large compute warehouses for dashboard queries
    
    **Query Pattern Optimization:**
    - Queries filtering by customer_id are automatically indexed
    - Aggregations (SUM, AVG, COUNT) are pre-computed and cached
    - Perfect for the "slice and dice" analytics shown in this dashboard
    - Designed for point lookups and key-based filtering on small-to-medium result sets
    """)
