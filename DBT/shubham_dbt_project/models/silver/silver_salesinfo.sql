WITH sales AS 
    (
        SELECT 
            sales_id,
            product_sk,
            customer_sk,
            {{ multiply ('unit_price','quantity')}} AS calculated_gross_amount,
            gross_amount,
            payment_method
        FROM
            {{ ref('bronze_sale') }}
    ),
    
    products AS 
    (
        SELECT 
        product_sk,
        category
        FROM    
            {{ref('bronze_products')}}

    ),
    customer AS
    (
        SELECT 
            customer_sk,
            gender
        FROM 
            {{ ref('bronze_customers') }}
    ),

    joined_query AS
    (

    SELECT 
        sales.sales_id,
        sales.gross_amount,
        sales.payment_method,
        products.category,
        customer.gender,
        calculated_gross_amount
    FROM
        sales
    JOIN
        products ON sales.product_sk = products.product_sk
    JOIN
        customer ON sales.customer_sk = customer.customer_sk

    )

    SELECT 
        category,
        gender,
        ROUND(SUM(gross_amount),0) AS total_sales
    FROM 
        joined_query
    GROUP BY 
        1,2
    ORDER By 
        3 DESC

