module.exports = {
    loadSchema: function() {
        return `
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
    `;
    },
    loadSchema_: function() {
        return `
        SELECT
            o.create_date,
            c.id,
            c.colid,
            RTRIM(o.[type]) [type],
            SCHEMA_NAME(o.schema_id) [namespace],
            o.Name AS [name],
            SCHEMA_NAME(o.schema_id) + '.' + o.Name AS [full],
            CASE o.[type]
                WHEN 'SN' THEN 'DROP SYNONYM [' + SCHEMA_NAME(o.schema_id) + '].[' + o.Name +
                                '] CREATE SYNONYM [' + SCHEMA_NAME(o.schema_id) + '].[' + o.Name + '] FOR ' +  s.base_object_name
                ELSE c.text
            END AS [source]

        FROM
            sys.objects o
        LEFT JOIN
            dbo.syscomments c on o.object_id = c.id
        LEFT JOIN
            sys.synonyms s on s.object_id = o.object_id
        WHERE
            o.type IN ('V', 'P', 'FN','F','IF','SN','TF','TR','U') AND
            user_name(objectproperty(o.object_id, 'OwnerId')) in (USER_NAME(),'dbo') AND
            objectproperty(o.object_id, 'IsMSShipped') = 0
        UNION ALL
        SELECT 0,0,0,'S',name,NULL,NULL,NULL FROM sys.schemas WHERE principal_id = USER_ID()
        UNION ALL
        SELECT
            0,0,0,'T',SCHEMA_NAME(t.schema_id)+'.'+t.name,NULL,NULL,NULL
        FROM
            sys.types t
        JOIN
            sys.schemas s ON s.principal_id = USER_ID() AND s.schema_id=t.schema_id
        WHERE
            t.is_user_defined=1
        ORDER BY
            1, 2, 3

        SELECT
            SCHEMA_NAME(types.schema_id) + '.' + types.name name,
            c.name [column],
            st.name type,
            CASE
                WHEN st.name in ('decimal','numeric') then CAST(c.[precision] AS VARCHAR)
                WHEN st.name in ('datetime2','time','datetimeoffset') then CAST(c.[scale] AS VARCHAR)
                WHEN st.name in ('varchar','varbinary') AND c.max_length>=0 THEN CAST(c.max_length as VARCHAR)
                WHEN st.name in ('nvarchar','nvarbinary') AND c.max_length>=0 THEN CAST(c.max_length/2 as VARCHAR)
                WHEN st.name in ('varchar','nvarchar','varbinary','nvarbinary') AND c.max_length<0 THEN 'max'
            END [length],
            CASE
                WHEN st.name in ('decimal','numeric') then c.scale
            END scale,
            object_definition(c.default_object_id) [default]
        FROM
            sys.table_types types
        JOIN
            sys.columns c ON types.type_table_object_id = c.object_id
        JOIN
            sys.systypes AS st ON st.xtype = c.system_type_id
        WHERE
            types.is_user_defined = 1 AND st.name <> 'sysname'
        ORDER BY
            1,c.column_id

        SELECT
            1 sort,
            s.name + '.' + o.name [name],
            'IF (OBJECT_ID(''[' + s.name + '].[' + o.name + ']'') IS NOT NULL) DROP PROCEDURE [' + s.name + '].[' + o.name + ']' [drop],
            p.name [param],
            SCHEMA_NAME(t.schema_id) + '.' + t.name [type]
        FROM
            sys.schemas s
        JOIN
            sys.objects o ON o.schema_id = s.schema_id
        JOIN
            sys.parameters p ON p.object_id = o.object_id
        JOIN
            sys.types t ON p.user_type_id = t.user_type_id AND t.is_user_defined=1
        WHERE
            user_name(objectproperty(o.object_id, 'OwnerId')) in (USER_NAME(),'dbo')
        UNION
        SELECT
            2,
            s.name + '.' + t.name [name],
            'DROP TYPE [' + s.name + '].[' + t.name + ']' [drop],
            NULL [param],
            SCHEMA_NAME(t.schema_id) + '.' + t.name [type]
        FROM
            sys.schemas s
        JOIN
            sys.types t ON t.schema_id=s.schema_id and t.is_user_defined=1
        WHERE
            user_name(s.principal_id) in (USER_NAME(),'dbo')
        ORDER BY 1`;
    },
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
    createUser: function(database, user, password) {
        return `
        DO
        $body$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_user WHERE usename = '${user}') THEN
                CREATE ROLE "${user}" LOGIN PASSWORD '${password}';
            END IF;
            GRANT CREATE ON DATABASE "${database}" TO "${user}";
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
