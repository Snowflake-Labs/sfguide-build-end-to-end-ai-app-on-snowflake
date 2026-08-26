-- ============================================================================
-- Cortex Agent: Business Insights Agent
-- ============================================================================
-- Creates an agent using the CREATE AGENT API that routes across:
--   1. Text-to-SQL (Cortex Analyst) via semantic view with verified queries
--   2. Agentic Search (multi-index Cortex Search over reviews + tickets)
--   3. Product catalog search
--
-- The agent automatically routes user questions to the right tool,
-- enabling "what happened → why" conversations that span structured
-- and unstructured data.
--
-- Prerequisites (created by setup.sql):
--   - DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_ANALYTICS_SEMANTIC
--   - DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMER_FEEDBACK_SEARCH
--   - DASH_AUTOMATED_INTELLIGENCE_DB.RAW.PRODUCT_SEARCH_SERVICE
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DASH_AUTOMATED_INTELLIGENCE_DB;
USE SCHEMA SEMANTIC;
USE WAREHOUSE HOL_WH;

CREATE OR REPLACE AGENT DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_INSIGHTS_AGENT
  COMMENT = 'Multi-tool business insights agent with Cortex Analyst and Agentic Search'
FROM SPECIFICATION $$
{
  "instructions": {
    "orchestration": "You are a business insights assistant for an outdoor sports equipment company selling skis, snowboards, boots, and accessories. Before calling any tool, explicitly state which tool you will use and why it is the right choice for this question. TOOL ROUTING RULES: (1) query_business_data: Use for ANY question about revenue, orders, customers, segments, discounts, metrics, counts, totals, trends, or aggregations over structured data. (2) search_customer_feedback: Use for ANY question about product reviews, customer feedback, ratings, support tickets, complaints, returns, sizing issues, quality problems, or shipping issues. This searches unstructured text across reviews and tickets. (3) search_products: Use for questions about product details, features, pricing, or catalog information. (4) COMBINE TOOLS: When a user asks WHY something happened (e.g. revenue dropped), use query_business_data first to quantify the impact, then search_customer_feedback to find qualitative explanations.",
    "response": "Be concise and data-driven. Always cite specific numbers from query results. When presenting search results, include relevant context like ratings, dates, and categories. Format currency values with $ and two decimal places. When combining multiple tool results, clearly connect the structured findings (what happened) with unstructured findings (why it happened).",
    "sample_questions": [
      {"question": "Show me monthly revenue trend from June to September 2025"},
      {"question": "Which month had the lowest revenue, and what do customer reviews say about that period?"},
      {"question": "Find reviews mentioning wrong size with a rating below 3"},
      {"question": "Why are customers returning ski boots?"},
      {"question": "What is our total revenue and customer count by state?"},
      {"question": "What are the top complaint themes in support tickets?"},
      {"question": "How many reviews mention sizing issues, and which products are most affected?"}
    ]
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "query_business_data",
        "description": "Query structured business data using natural language. Covers orders, revenue, customers, segments, products, discounts, shipping, and order status. Use for questions about metrics, trends, aggregations, comparisons, and any quantitative business analysis."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "search_customer_feedback",
        "description": "Search across customer reviews and support tickets for qualitative insights. IMPORTANT: always use persist_to_table. Use for finding reviews about specific products, quality issues, sizing problems, customer sentiment, support tickets about returns, shipping delays, or any customer complaints."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "search_products",
        "description": "Search the product catalog for product details, descriptions, features, pricing, and categories. Use for questions about what products are available, product specs, or product comparisons."
      }
    }
  ],
  "tool_resources": {
    "query_business_data": {
      "semantic_view": "DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_ANALYTICS_SEMANTIC",
      "execution_environment": {"type": "warehouse", "warehouse": "HOL_WH"}
    },
    "search_customer_feedback": {
      "search_service": "DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMER_FEEDBACK_SEARCH",
      "database_schema": "DASH_AUTOMATED_INTELLIGENCE_DB.RAW",
      "is_multi_index": true,
      "columns_and_descriptions": {
        "doc_id": {"description": "Unique document identifier (review_id or ticket_id)", "type": "TEXT", "searchable": false, "filterable": false},
        "title": {"description": "Review title or ticket subject line", "type": "TEXT", "searchable": true, "filterable": false},
        "content": {"description": "Full text of the review or support ticket description", "type": "TEXT", "searchable": true, "filterable": false},
        "source_type": {"description": "Type of document: review or ticket", "type": "TEXT", "searchable": false, "filterable": true},
        "category": {"description": "Ticket category such as Returns, Shipping, Sizing (null for reviews)", "type": "TEXT", "searchable": false, "filterable": true},
        "rating": {"description": "Review star rating 1-5 (null for tickets)", "type": "NUMBER", "searchable": false, "filterable": true},
        "date_field": {"description": "Date when the review was posted or ticket was created", "type": "DATE", "searchable": false, "filterable": true},
        "customer_id": {"description": "Customer identifier", "type": "NUMBER", "searchable": false, "filterable": true}
      },
      "max_results": 1000,
      "execution_environment": {"type": "warehouse", "warehouse": "HOL_WH"},
      "id_column": "DOC_ID",
      "base_table": "DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMER_FEEDBACK",
      "base_table_columns": ["doc_id", "title", "content", "source_type", "category", "rating", "date_field", "customer_id"]
    },
    "search_products": {
      "search_service": "DASH_AUTOMATED_INTELLIGENCE_DB.RAW.PRODUCT_SEARCH_SERVICE",
      "id_column": "PRODUCT_ID",
      "title_column": "PRODUCT_NAME",
      "columns_and_descriptions": {
        "product_id": {"description": "Unique product identifier", "type": "TEXT", "searchable": false, "filterable": false},
        "product_name": {"description": "Product name", "type": "TEXT", "searchable": true, "filterable": false},
        "product_category": {"description": "Product category such as Skis, Boots, Accessories", "type": "TEXT", "searchable": false, "filterable": true},
        "features": {"description": "Product features and specifications", "type": "TEXT", "searchable": true, "filterable": false},
        "price": {"description": "Product price in USD", "type": "NUMBER", "searchable": false, "filterable": true}
      },
      "max_results": 100,
      "execution_environment": {"type": "warehouse", "warehouse": "HOL_WH"}
    }
  }
}
$$;

-- Verify Agent
SHOW AGENTS LIKE 'BUSINESS_INSIGHTS_AGENT' IN SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC;

-- Set agent profile for Snowflake CoWork display
ALTER AGENT DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_INSIGHTS_AGENT
  SET PROFILE = '{"display_name": "Business Insights", "color": "#29B5E8"}';

-- Grant WEST_COAST_MANAGER access to the agent (for Security & Governance demo)
GRANT USAGE ON SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC TO ROLE WEST_COAST_MANAGER;
GRANT USAGE ON AGENT DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_INSIGHTS_AGENT TO ROLE WEST_COAST_MANAGER;

-- Make agent visible in Snowflake CoWork
-- On fresh accounts (no SI object), agents auto-appear — no action needed.
-- If the account already has a SI object, uncomment the following:
-- ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_INSIGHTS_AGENT;

-- ============================================================================
-- Sample Questions for the Agent
-- ============================================================================
--
-- "WHAT" questions (structured data via text-to-SQL):
--   "What was our total revenue in December 2025?"
--   "What are the top 5 product categories by order count?"
--   "What is the cancellation rate by month?"
--
-- "WHY" questions (unstructured search):
--   "Why are customers returning ski boots?"
--   "What are customers complaining about in February?"
--   "Show me support tickets about sizing issues"
--
-- "WHAT → WHY" multi-turn conversations:
--   User: "What happened to revenue in February vs January?"
--   Agent: Revenue dropped 38% ($78M → $32M). Cancellations spiked to 12%.
--   User: "Why did that happen?"
--   Agent: [searches reviews + tickets] Boot sizing complaints drove returns.
--          15 negative reviews mention "wrong size" and "tight fit".
--          40 support tickets filed for exchanges in Feb (vs 8 in Jan).
--
-- Multi-index search (keyword + semantic):
--   "Find reviews mentioning 'wrong size' with a rating below 3"
--   "Search for tickets about shipping delays during the holiday season"
--   "What do customers say about the Powder Skis quality?"
-- ============================================================================
