-- ============================================================================
-- Agent Evaluation: BUSINESS_INSIGHTS_AGENT
-- ============================================================================
-- Creates the evaluation dataset (ground truth).
-- The evaluation itself is best run via Snowsight UI:
--   AI & ML → Agents → BUSINESS_INSIGHTS_AGENT → Evaluations → New evaluation run
--
-- NOTE: The programmatic EXECUTE_AI_EVALUATION API does not yet support
-- Agentic Search (is_multi_index) tool_resources. Use the Snowsight UI instead.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DASH_AUTOMATED_INTELLIGENCE_DB;
USE SCHEMA SEMANTIC;
USE WAREHOUSE HOL_WH;

-- ============================================================================
-- STEP 1: Create evaluation dataset table
-- ============================================================================

CREATE OR REPLACE TABLE agent_evaluation_data (
    input_query VARCHAR,
    ground_truth VARIANT
);

INSERT INTO agent_evaluation_data
SELECT 'Show me monthly revenue trend from August to November 2026',
       PARSE_JSON('{"ground_truth_output": "The agent should query structured business data using the query_business_data tool to produce a monthly revenue breakdown. The response should include revenue figures for each month from August through November 2026, showing growth from August/September through the November holiday peak."}') 
UNION ALL SELECT 'Revenue dipped in October compared to November — what caused it and what do reviews say?',
       PARSE_JSON('{"ground_truth_output": "The agent should use BOTH query_business_data and search_customer_feedback. The response should connect the quantitative difference between October and November to qualitative reasons from customer reviews or support tickets."}') 
UNION ALL SELECT 'Find reviews mentioning wrong size with a rating below 3',
       PARSE_JSON('{"ground_truth_output": "The agent should use search_customer_feedback to find product reviews that mention sizing issues with low ratings below 3. Results should include specific review content, product names, and ratings."}') 
UNION ALL SELECT 'Why are customers returning ski boots?',
       PARSE_JSON('{"ground_truth_output": "The agent should search customer feedback related to ski boot returns. The response should identify common return reasons such as sizing issues, comfort problems, or quality concerns, citing specific customer feedback."}') 
UNION ALL SELECT 'What is our total revenue and customer count by state?',
       PARSE_JSON('{"ground_truth_output": "The agent should use query_business_data to aggregate total revenue and distinct customer count grouped by state. The response should include states with their corresponding revenue and customer counts."}') 
UNION ALL SELECT 'What are the top complaint themes in support tickets from October 2026?',
       PARSE_JSON('{"ground_truth_output": "The agent should use search_customer_feedback to find and analyze support tickets from October 2026. The response should identify main complaint themes with examples from actual tickets."}') 
UNION ALL SELECT 'How many reviews mention sizing issues, and which products are most affected?',
       PARSE_JSON('{"ground_truth_output": "The agent should use search_customer_feedback to find reviews mentioning sizing issues. The response should provide a count and identify which products are most frequently mentioned in sizing complaints."}');

SELECT COUNT(*) as evaluation_questions FROM agent_evaluation_data;

-- ============================================================================
-- STEP 2: Create evaluation stage and upload YAML config
-- ============================================================================

CREATE OR REPLACE FILE FORMAT yaml_file_format
  TYPE = 'CSV'
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE
  ESCAPE_UNENCLOSED_FIELD = NONE;

CREATE OR REPLACE STAGE evaluation_config
  FILE_FORMAT = yaml_file_format;

-- Upload the YAML config (agent_evaluation_config.yaml) to stage
-- From Snowflake CoCo / CLI:
--   snow stage copy snowflake-cowork/agent_evaluation_config.yaml @DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.EVALUATION_CONFIG/ --overwrite
-- Or from Snowsight: Ingestion > Add Data > Load files into a Stage

PUT file://agent_evaluation_config.yaml @evaluation_config AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

LIST @evaluation_config;

-- ============================================================================
-- STEP 3: Run the evaluation
-- ============================================================================

CALL EXECUTE_AI_EVALUATION(
  'START',
  OBJECT_CONSTRUCT('run_name', 'hol-eval-run-1'),
  '@DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.EVALUATION_CONFIG/agent_evaluation_config.yaml'
);

-- ============================================================================
-- STEP 4: Check evaluation status (run after ~2-5 minutes)
-- ============================================================================

-- CALL EXECUTE_AI_EVALUATION(
--   'STATUS',
--   OBJECT_CONSTRUCT('run_name', 'hol-eval-run-1'),
--   '@DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.EVALUATION_CONFIG/agent_evaluation_config.yaml'
-- );

-- ============================================================================
-- STEP 5: Inspect evaluation results (run after evaluation completes)
-- ============================================================================

-- SELECT * FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_EVALUATION_DATA(
--   'DASH_AUTOMATED_INTELLIGENCE_DB',
--   'SEMANTIC',
--   'BUSINESS_INSIGHTS_AGENT',
--   'CORTEX AGENT',
--   'hol-eval-run-1')
-- );
