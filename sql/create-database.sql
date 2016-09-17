DO
$do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = ${database}) THEN
        PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE ${database~}');
    END IF;
END
$do$
