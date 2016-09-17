--schemas
SELECT
    'S' "type", n.nspname AS "namespace", NULL "name", NULL "full", NULL "source", NULL "params" FROM pg_catalog.pg_namespace AS n
WHERE
     n.nspname NOT IN ('pg_catalog', 'information_schema')
UNION

--tables
SELECT
    'U' "type", n.nspname, c.relname, n.nspname || '.' || c.relname, NULL, NULL FROM pg_catalog.pg_class as c LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema') AND c.relkind in ('r', 'v', 'm', 'f')
UNION

--functions
SELECT DISTINCT
    'P' "type",
    n.nspname,
    p.proname,
    n.nspname || '.' || p.proname AS "full",
    p.prosrc AS "source",
    pg_catalog.pg_get_function_identity_arguments(p.oid) AS "params"
            -- ,(not p.proretset) AS "returnsSingleRow"
            -- ,(t.typtype in ('b', 'd', 'e', 'r')) AS "returnsSingleValue"
            -- ,p.pronargs AS "paramsCount"
            -- ,r.routine_definition
FROM pg_proc p
    INNER JOIN pg_namespace n ON (p.pronamespace = n.oid)
    INNER JOIN pg_type t ON (p.prorettype = t.oid)
        -- INNER JOIN information_schema.routines r ON r.routine_name = p.proname
    WHERE n.nspname NOT IN ('pg_catalog','information_schema')
