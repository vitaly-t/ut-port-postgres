module.exports = {
    createDatabase: function(name) {
        return `
        DO
        $do$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = '${name}') THEN
                PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE "${name}"');
            END IF;
        END
        $do$
        `;
    },
    createUser: function(name, user, password) {
        return `
        DO
        $body$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_user WHERE usename = '${user}') THEN
                CREATE ROLE "${user}" LOGIN PASSWORD '${password}';
            END IF;
        END
        $body$;
        `;
    },
    getFunctions: function() {
        return `
        SELECT DISTINCT
            n.nspname AS "schema"
            -- ,(not p.proretset) AS "returnsSingleRow"
            -- ,(t.typtype in ('b', 'd', 'e', 'r')) AS "returnsSingleValue"
            ,p.proname AS "name"
            -- ,p.pronargs AS "paramsCount"
            ,pg_catalog.pg_get_function_identity_arguments(p.oid) AS "params"
            ,p.prosrc AS "source"
            -- ,r.routine_definition
        FROM pg_proc p
            INNER JOIN pg_namespace n ON (p.pronamespace = n.oid)
            INNER JOIN pg_type t ON (p.prorettype = t.oid)
            -- INNER JOIN information_schema.routines r ON r.routine_name = p.proname
        WHERE n.nspname NOT IN ('pg_catalog','information_schema')
        AND n.nspname NOT LIKE 'pgp%'
        ORDER BY n.nspname, p.proname
        `;
    }
};
