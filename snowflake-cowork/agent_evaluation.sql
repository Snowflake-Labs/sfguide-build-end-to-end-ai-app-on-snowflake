-- ============================================================================
-- Agent Evaluation: BUSINESS_INSIGHTS_AGENT
-- ============================================================================
-- setup.sql creates the ground-truth table (SEMANTIC.AGENT_EVALUATION_DATA) and
-- registers it as the HOL_EVAL_DATASET evaluation dataset.
-- The evaluation itself is best run via Snowsight UI:
--   AI & ML > Agent Studio (Agents in some accounts) > BUSINESS_INSIGHTS_AGENT
--   > Evaluations > Run an evaluation manually > Existing dataset > HOL_EVAL_DATASET
--
-- NOTE: The programmatic EXECUTE_AI_EVALUATION API does not yet support
-- Agentic Search (is_multi_index) tool_resources. Use the Snowsight UI instead.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DASH_AUTOMATED_INTELLIGENCE_DB;
USE SCHEMA SEMANTIC;
USE WAREHOUSE HOL_WH;

-- ============================================================================
-- STEP 1: Confirm the evaluation dataset exists (created by setup.sql)
-- ============================================================================

SHOW DATASETS LIKE 'HOL_EVAL_DATASET' IN SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC;
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
