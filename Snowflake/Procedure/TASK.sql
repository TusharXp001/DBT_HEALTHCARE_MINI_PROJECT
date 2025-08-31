CREATE OR REPLACE TASK load_stage_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
CALL load_stage_files();


ALTER TASK load_stage_task RESUME;

show tasks;