SELECT 
    *
FROM 
{{ ref('bronze_sale') }}
WHERE
    gross_amount < 0 AND net_amount < 0