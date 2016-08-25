# **Postgres Port:** `ut-port-postgres`

## Returning multiple result sets

If there is need to return data from several unrelated queries, the following pattern can be used.
If we have the queries q1 and q2 (each one is any valid select statement represented with SELECT 1 and SELECT 2 below), then the following can be executed:

```sql
WITH
    q1 AS (SELECT 1),
    q2 AS (SELECT 2)
SELECT
    (SELECT json_agg(q1) FROM q1) AS q1,
    (SELECT json_agg(q2) FROM q2) AS q2
```

This query can be used as result from a function with the following signature:

```sql
CREATE FUNCTION f1() RETURNS TABLE(q1 json, q2 json) AS
```

The result returned by the port will be JSON with the following structure:

```json
{
    "q1":[],
    "q2":[]
}
```

This basically returns two result sets as properties in one object. Each result set is array of objects, where each object represents one row from the query.
