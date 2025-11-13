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
    CUSTOMER_FNAME,
    CUSTOMER_SEGMENT,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_COUNTRY
FROM myDataSource;

-- -----------------------------------------------------
-- PRODUCT_DIM Transformation
-- -----------------------------------------------------
SELECT DISTINCT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY_ID,
    CATEGORY_NAME,
    PRODUCT_PRICE
FROM myDataSource;

-- -----------------------------------------------------
-- DELIVERY_DIM Transformation
-- -----------------------------------------------------
SELECT DISTINCT
    ORDER_ID,
    SHIPPING_DATE,
    DELIVERY_STATUS,
    SHIPPING_MODE,
    SHIPPING_DAYS
FROM myDataSource;


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
    SUM(p.PRODUCT_PRICE * o.ORDER_QUANTITY) AS TOTAL_SALES,
    SUM(p.PRODUCT_PRICE * o.ORDER_QUANTITY * o.DISCOUNT_PERCENTAGE / 100) AS TOTAL_DISCOUNT_AMOUNT,
    AVG(o.DISCOUNT_RATE) AS AVG_DISCOUNT_RATE,
    AVG(o.PROFIT_RATIO) AS AVG_PROFIT_RATIO,
    CASE 
        WHEN SUM(o.ORDER_QUANTITY) >= 10 THEN 'Large Order'
        WHEN SUM(o.ORDER_QUANTITY) BETWEEN 5 AND 9 THEN 'Medium Order'
        ELSE 'Small Order'
    END AS ORDER_SIZE_CATEGORY,
    o.PAYMENT_TYPE,
    o.ORDER_STATUS
FROM orderDim o
INNER JOIN customerDim c ON o.ORDER_CUSTOMER_ID = c.CUSTOMER_ID
INNER JOIN productDim p ON o.ORDER_PRODUCT_ID = p.PRODUCT_ID
INNER JOIN deliveryDim d ON o.ORDER_ID = d.ORDER_ID
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