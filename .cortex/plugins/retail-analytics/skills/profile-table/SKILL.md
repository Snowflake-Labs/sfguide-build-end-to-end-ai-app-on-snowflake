# Profile Table

Profiles a Snowflake table: row count, NULL columns, distinct values, and data quality flags.

## Triggers
- profile-table
- profile table
- $profile-table

## Steps
1. Run `SELECT COUNT(*) FROM <table>`
2. Check for NULL columns using `NULL_COUNT`
3. Show distinct value counts for categorical columns
4. Flag any columns with >5% NULLs as potential quality issues
