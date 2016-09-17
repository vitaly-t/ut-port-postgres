DO
$body$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_user WHERE usename = ${user}) THEN
        CREATE ROLE ${user~} LOGIN PASSWORD ${password};
    END IF;
    GRANT CREATE ON DATABASE ${database~} TO ${user~};
END
$body$
