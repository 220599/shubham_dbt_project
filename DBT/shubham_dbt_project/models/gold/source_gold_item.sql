with dedup_query AS
(

SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION By id ORDER BY updatedate DESC) AS dedup
 FROM 
    {{ source('source', 'item') }}

)
SELECT ID, name, updatedate
FROM dedup_query
WHERE 
    dedup = 1
