-- 0019_drop_process_paths.up.sql: ADR 0025 stage 3. The local process-path
-- declaration retired in stage 2 (POST /process-paths answers 410); the paths
-- a plan composes now come only from the process flows consumed from
-- process-path-management (0018). The table is empty by then: every read
-- resolved flows first since stage 1.
DROP TABLE IF EXISTS process_paths;
