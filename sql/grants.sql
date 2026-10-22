-- The state machine reaches Redshift through the Data API as the database user "IAMR:<role name>".
-- __SFN_ROLE__ is replaced by scripts/setup_warehouse.py with the role name from the foundation stack.
-- Run as the namespace admin. CREATE USER errors if the user already exists; the script tolerates that.
CREATE USER "IAMR:__SFN_ROLE__" PASSWORD DISABLE;
GRANT USAGE ON SCHEMA public TO "IAMR:__SFN_ROLE__";
GRANT ALL ON ALL TABLES IN SCHEMA public TO "IAMR:__SFN_ROLE__";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "IAMR:__SFN_ROLE__"
