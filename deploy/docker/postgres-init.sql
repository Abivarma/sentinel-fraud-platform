-- Create hive_metastore database for Spark catalog
SELECT 'CREATE DATABASE hive_metastore'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'hive_metastore')\gexec
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO airflow;
