import streamlit as st
import altair as alt
from shared import get_session, show_header, format_number

show_header()
st.subheader("🔄 Data Pipeline")
st.divider()

session = get_session()

def render_dashboard(schema: str, orders_table: str, order_items_table: str, show_customers: bool = True):
    staging_count = 0
    try:
        if show_customers:
            col1, col2, col3, col4, col5 = st.columns(5)
        else:
            col1, col2, col3, col4 = st.columns(4)

        orders_cnt = session.sql(f"SELECT COUNT(*) as cnt FROM DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{orders_table}").collect()[0]['CNT']
        staging_count = orders_cnt
        items_cnt = session.sql(f"SELECT COUNT(*) as cnt FROM DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{order_items_table}").collect()[0]['CNT']
        revenue = session.sql(f"SELECT COALESCE(ROUND(SUM(total_amount), 2), 0) as val FROM DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{orders_table}").collect()[0]['VAL']
        products_cnt = session.sql(f"SELECT COUNT(DISTINCT product_id) as cnt FROM DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{order_items_table}").collect()[0]['CNT']

        if show_customers:
            customers_cnt = session.sql("SELECT COUNT(*) as cnt FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMERS").collect()[0]['CNT']
            col1.metric("Customers", format_number(customers_cnt, include_decimals=False))
            col2.metric("Orders", format_number(orders_cnt, include_decimals=False))
            col3.metric("Order Items", format_number(items_cnt, include_decimals=False))
            col4.metric("Products", format_number(products_cnt, include_decimals=False))
            col5.metric("Revenue", f"${format_number(revenue)}")
        else:
            col1.metric("Orders", format_number(orders_cnt, include_decimals=False))
            col2.metric("Order Items", format_number(items_cnt, include_decimals=False))
            col3.metric("Products", format_number(products_cnt, include_decimals=False))
            col4.metric("Revenue", f"${format_number(revenue)}")

        st.divider()

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### 📊 Order Status Distribution")
            status_df = session.sql(f"""
                SELECT ORDER_STATUS, COUNT(*) as order_count
                FROM DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{orders_table}
                GROUP BY ORDER_STATUS ORDER BY order_count DESC
            """).to_pandas()

            if not status_df.empty:
                fig = alt.Chart(status_df).mark_arc().encode(
                    theta=alt.Theta("ORDER_COUNT:Q"),
                    color=alt.Color("ORDER_STATUS:N", legend=alt.Legend(title="Order Status")),
                    tooltip=["ORDER_STATUS", "ORDER_COUNT"],
                ).properties(height=400)
                st.altair_chart(fig, use_container_width=True)
            else:
                st.info("⚠️ No data available.")

        with col_right:
            st.markdown("#### 📦 Product Category Revenue")
            cat_df = session.sql(f"""
                SELECT product_category, SUM(line_total) as total_revenue
                FROM DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{order_items_table}
                GROUP BY product_category ORDER BY total_revenue DESC
            """).to_pandas()

            if not cat_df.empty:
                fig = alt.Chart(cat_df).mark_bar().encode(
                    x=alt.X("PRODUCT_CATEGORY:N", title="Category"),
                    y=alt.Y("TOTAL_REVENUE:Q", title="Revenue"),
                    tooltip=["PRODUCT_CATEGORY", "TOTAL_REVENUE"],
                ).properties(height=400)
                st.altair_chart(fig, use_container_width=True)
            else:
                st.info("⚠️ No data available.")

        st.divider()
        st.markdown("#### 📊 Product Category Sales by Order Size")

        stacked_df = session.sql(f"""
            SELECT 
                CASE
                    WHEN o.total_amount < 100 THEN 'Small (<$100)'
                    WHEN o.total_amount < 500 THEN 'Medium ($100-$500)'
                    WHEN o.total_amount < 2000 THEN 'Large ($500-$2K)'
                    ELSE 'Extra Large (>$2K)'
                END AS order_size,
                oi.product_category,
                SUM(oi.line_total) as revenue
            FROM DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{orders_table} o
            JOIN DASH_AUTOMATED_INTELLIGENCE_DB.{schema}.{order_items_table} oi ON o.order_id = oi.order_id
            GROUP BY order_size, oi.product_category
            ORDER BY 
                CASE order_size
                    WHEN 'Small (<$100)' THEN 1
                    WHEN 'Medium ($100-$500)' THEN 2
                    WHEN 'Large ($500-$2K)' THEN 3
                    WHEN 'Extra Large (>$2K)' THEN 4
                END,
                oi.product_category
        """).to_pandas()

        if not stacked_df.empty:
            size_order = ['Small (<$100)', 'Medium ($100-$500)', 'Large ($500-$2K)', 'Extra Large (>$2K)']
            fig = alt.Chart(stacked_df).mark_bar().encode(
                x=alt.X("ORDER_SIZE:N", title="Order Size", sort=size_order),
                y=alt.Y("REVENUE:Q", title="Revenue"),
                color=alt.Color("PRODUCT_CATEGORY:N", title="Category"),
                tooltip=["ORDER_SIZE", "PRODUCT_CATEGORY", "REVENUE"],
            ).properties(height=500)
            st.altair_chart(fig, use_container_width=True)
        else:
            st.info("⚠️ No data available. Stream data first using Snowpipe Streaming.")

    except Exception as e:
        st.error(str(e))
    return staging_count

tab_staging, tab_production = st.tabs(["🔧 Staging (Live Ingestion)", "🚀 Production"])

with tab_staging:
    st.caption("Data lands here via **Snowpipe Streaming** → Merge promotes to Production")
    staging_rows = render_dashboard("STAGING", "ORDERS_STAGING", "ORDER_ITEMS_STAGING", show_customers=False)

    st.divider()
    st.subheader("🔄 Promote Staging → Production (MERGE)")
    st.markdown("""
This operation de-duplicates staging data (using `ROW_NUMBER`), then performs an **upsert** into 
production tables — inserting new records and updating existing ones. After a successful merge, 
confirmed rows are deleted from staging.
""")
    st.caption("⚡ Executed on **Gen2 Warehouse** (`HOL_GEN2_WH`) for optimized indexing")

    with st.expander("View MERGE Procedure SQL", expanded=False):
        st.code("""MERGE INTO raw.orders tgt
USING (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) as rn
        FROM staging.orders_staging
    )
    WHERE rn = 1
) src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

MERGE INTO raw.order_items tgt
USING (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY order_item_id) as rn
        FROM staging.order_items_staging
    )
    WHERE rn = 1
) src
ON tgt.order_item_id = src.order_item_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;""", language="sql")

    merge_done = st.session_state.get("merge_done", False)
    has_data = staging_rows > 0

    if merge_done:
        st.success("✅ MERGE complete! Switch to **Production** tab to see updated data.")
        if st.button("🔄 Reset"):
            st.session_state["merge_done"] = False
            st.rerun()
    else:
        if not has_data:
            st.info("No data in staging to merge. Start Snowpipe Streaming to ingest data.")
        if st.button("▶️ Run MERGE (Promote to Production)", type="primary", disabled=not has_data):
            with st.spinner("Running MERGE on Gen2 Warehouse..."):
                try:
                    session.sql("USE WAREHOUSE HOL_GEN2_WH").collect()
                    result = session.sql("CALL DASH_AUTOMATED_INTELLIGENCE_DB.STAGING.MERGE_STAGING_TO_RAW(TRUE)").collect()
                    session.sql("USE WAREHOUSE HOL_WH").collect()
                    st.session_state["merge_done"] = True
                    st.rerun()
                except Exception as e:
                    st.error(f"MERGE failed: {e}")

with tab_production:
    st.caption("📅 All-time historical data (after MERGE from Staging)")
    render_dashboard("RAW", "ORDERS", "ORDER_ITEMS")
