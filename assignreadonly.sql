CREATE USER assignment_readonly WITH PASSWORD 'password';


GRANT CONNECT ON DATABASE jaffle_shop TO assignment_readonly;


GRANT USAGE ON SCHEMA jaffle_data TO assignment_readonly;


GRANT SELECT ON ALL TABLES IN SCHEMA jaffle_data TO assignment_readonly;


ALTER DEFAULT PRIVILEGES IN SCHEMA jaffle_data
GRANT SELECT ON TABLES TO assignment_readonly; 