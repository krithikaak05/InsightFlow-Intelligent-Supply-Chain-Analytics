-- =====================================================
-- InsightFlow: ETL SQL Queries
-- =====================================================

-- =====================================================
-- ETL JOB 1: RAW DATA TO DIMENSION TABLES
-- =====================================================

-- -----------------------------------------------------
-- ORDER_DIM Transformation
-- -----------------------------------------------------
SELECT DISTINCT
    LPAD(CAST(ORDER_ID AS VARCHAR(20)), 10, '0') AS ORDER_ID,
    ORDER_CUSTOMER_ID,
    ORDER_PRODUCT_ID,
    COALESCE(ORDER_QUANTITY, 0) AS ORDER_QUANTITY,
    COALESCE(DISCOUNT_PERCENTAGE, 0) AS DISCOUNT_PERCENTAGE,
    COALESCE(DISCOUNT_RATE, 0.0) AS DISCOUNT_RATE,
    COALESCE(PROFIT_RATIO, 0.0) AS PROFIT_RATIO,
    COALESCE(PAYMENT_TYPE, 'Unknown') AS PAYMENT_TYPE,
    COALESCE(ORDER_STATUS, 'Pending') AS ORDER_STATUS,
    PRODUCT_PRICE
FROM myDataSource;

-- -----------------------------------------------------
-- CUSTOMER_DIM Transformation
-- -----------------------------------------------------
SELECT DISTINCT
    CUSTOMER_ID,
    CONCAT(UPPER(SUBSTRING(CUSTOMER_FNAME, 1, 1)), LOWER(SUBSTRING(CUSTOMER_FNAME, 2))) AS CUSTOMER_FNAME,
    CONCAT(UPPER(SUBSTRING(CUSTOMER_LNAME, 1, 1)), LOWER(SUBSTRING(CUSTOMER_LNAME, 2))) AS CUSTOMER_LNAME,
    COALESCE(CUSTOMER_SEGMENT, 'Unknown') AS CUSTOMER_SEGMENT,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_COUNTY,
    CUSTOMER_ZIPCODE,
    CASE 
        WHEN CUSTOMER_SEGMENT = 'Corporate' THEN 'High Value'
        WHEN CUSTOMER_SEGMENT = 'Consumer' THEN 'Regular Value'
        ELSE 'Low Value'
    END AS CUSTOMER_CATEGORY
FROM myDataSource;

-- -----------------------------------------------------
-- PRODUCT_DIM Transformation
-- -----------------------------------------------------
SELECT DISTINCT
    PRODUCT_ID,
    CONCAT(UPPER(SUBSTRING(PRODUCT_NAME, 1, 1)), LOWER(SUBSTRING(PRODUCT_NAME, 2))) AS PRODUCT_NAME,
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    CATEGORY_ID,
    CATEGORY_NAME,
    CASE 
        WHEN DEPARTMENT_NAME = 'Electronics' THEN 'High Priority'
        ELSE 'Regular Priority'
    END AS PRODUCT_PRIORITY
FROM myDataSource;

-- -----------------------------------------------------
-- DELIVERY_DIM Transformation
-- -----------------------------------------------------
SELECT DISTINCT
    LPAD(CAST(order_id AS VARCHAR(20)), 10, '0') AS DELIVERY_ID,
    shipping_days AS SHIPPING_DAYS,
    delivery_status,
    CAST(late_delivery_risk AS INT) AS LATE_DELIVERY_RISK,
    TO_DATE(shipping_date, 'M/d/yy') AS SHIPPING_DATE,
    CASE
        WHEN shipping_mode IS NOT NULL THEN CONCAT(UPPER(SUBSTRING(shipping_mode, 1, 1)), LOWER(SUBSTRING(shipping_mode, 2)))
        ELSE 'Unknown'
    END AS SHIPPING_MODE,
    CASE 
        WHEN late_delivery_risk = 1 THEN TRUE
        ELSE FALSE
    END AS IS_LATE_DELIVERY,
    CASE
        WHEN shipping_days <= 2 THEN 'Fast'
        WHEN shipping_days BETWEEN 3 AND 5 THEN 'Standard'
        ELSE 'Slow'
    END AS DELIVERY_CATEGORY
FROM myDataSource
WHERE shipping_date IS NOT NULL;


-- =====================================================
-- ETL JOB 2: DIMENSIONS TO FACT TABLE
-- =====================================================

-- -----------------------------------------------------
-- FACT_SALES Creation
-- -----------------------------------------------------
SELECT 
    ROW_NUMBER() OVER (ORDER BY o.ORDER_ID) AS FACT_ID,
    o.ORDER_ID,
    o.ORDER_CUSTOMER_ID,
    c.CUSTOMER_FNAME,
    c.CUSTOMER_SEGMENT,
    o.ORDER_PRODUCT_ID,
    p.PRODUCT_NAME,
    d.SHIPPING_DATE,
    d.DELIVERY_STATUS,
    d.SHIPPING_MODE,
    SUM(o.ORDER_QUANTITY) AS TOTAL_ORDER_QUANTITY,
    SUM(o.PRODUCT_PRICE * o.ORDER_QUANTITY) AS TOTAL_SALES,
    SUM(o.PRODUCT_PRICE * o.ORDER_QUANTITY * o.DISCOUNT_PERCENTAGE / 100) AS TOTAL_DISCOUNT_AMOUNT,
    AVG(o.DISCOUNT_RATE) AS AVG_DISCOUNT_RATE,
    AVG(o.PROFIT_RATIO) AS AVG_PROFIT_RATIO,
    CASE 
        WHEN SUM(o.ORDER_QUANTITY) >= 10 THEN 'Large Order'
        WHEN SUM(o.ORDER_QUANTITY) BETWEEN 5 AND 9 THEN 'Medium Order'
        ELSE 'Small Order'
    END AS ORDER_SIZE_CATEGORY,
    o.PAYMENT_TYPE,
    o.ORDER_STATUS
FROM ORDER_DIM o
INNER JOIN CUSTOMER_DIM c ON o.ORDER_CUSTOMER_ID = c.CUSTOMER_ID
INNER JOIN PRODUCT_DIM p ON o.ORDER_PRODUCT_ID = p.PRODUCT_ID
INNER JOIN DELIVERY_DIM d ON o.ORDER_ID = d.DELIVERY_ID
GROUP BY 
    o.ORDER_ID,
    o.ORDER_CUSTOMER_ID,
    c.CUSTOMER_FNAME,
    c.CUSTOMER_SEGMENT,
    o.ORDER_PRODUCT_ID,
    p.PRODUCT_NAME,
    d.SHIPPING_DATE,
    d.DELIVERY_STATUS,
    d.SHIPPING_MODE,
    o.PAYMENT_TYPE,
    o.ORDER_STATUS;