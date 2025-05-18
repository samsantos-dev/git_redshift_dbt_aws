
WITH tabela1 AS (
  SELECT *,
         FIRST_VALUE(customer_id) OVER (
             PARTITION BY company_name, contact_name 
             ORDER BY company_name ASC 
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS fv
  FROM {{ source('sources', 'customers') }}
),
tabela2 AS (
  SELECT DISTINCT fv 
  FROM tabela1
)
SELECT 
        c.customer_id,
        c.company_name,
        c.contact_name,
        c.address,
        c.city,
        c.postal_code,
        c.country,
        c.phone
FROM {{ source('sources', 'customers') }} AS c
JOIN tabela2 AS t
ON c.customer_id = t.fv
/*
WITH tabela1 AS (
 SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY company_name, contact_name 
            ORDER BY company_name ASC
        ) AS row_num
 FROM customers
)
SELECT *
FROM tabela1
WHERE row_num = 1
*/