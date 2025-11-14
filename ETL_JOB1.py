import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node raw_data_order_data
raw_data_order_data_node1732404430093 = glueContext.create_dynamic_frame.from_catalog(database="datawarehousing", table_name="rawdata_order", transformation_ctx="raw_data_order_data_node1732404430093")

# Script generated for node raw_data_customer_data
raw_data_customer_data_node1732403274243 = glueContext.create_dynamic_frame.from_catalog(database="datawarehousing", table_name="rawdata_customer", transformation_ctx="raw_data_customer_data_node1732403274243")

# Script generated for node raw_data_delivery_data
raw_data_delivery_data_node1732404426677 = glueContext.create_dynamic_frame.from_catalog(database="datawarehousing", table_name="rawdata_delivery", transformation_ctx="raw_data_delivery_data_node1732404426677")

# Script generated for node raw_data_product_data
raw_data_product_data_node1732404430681 = glueContext.create_dynamic_frame.from_catalog(database="datawarehousing", table_name="rawdata_product", transformation_ctx="raw_data_product_data_node1732404430681")

# Script generated for node SQL Query
SqlQuery81 = '''
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
'''
SQLQuery_node1732406862433 = sparkSqlQuery(glueContext, query = SqlQuery81, mapping = {"myDataSource":raw_data_order_data_node1732404430093}, transformation_ctx = "SQLQuery_node1732406862433")

# Script generated for node SQL Query
SqlQuery80 = '''
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
'''
SQLQuery_node1732403451704 = sparkSqlQuery(glueContext, query = SqlQuery80, mapping = {"myDataSource":raw_data_customer_data_node1732403274243}, transformation_ctx = "SQLQuery_node1732403451704")

# Script generated for node SQL Query
SqlQuery79 = '''
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
'''
SQLQuery_node1732406736707 = sparkSqlQuery(glueContext, query = SqlQuery79, mapping = {"myDataSource":raw_data_delivery_data_node1732404426677}, transformation_ctx = "SQLQuery_node1732406736707")

# Script generated for node SQL Query
SqlQuery78 = '''
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
'''
SQLQuery_node1732406677413 = sparkSqlQuery(glueContext, query = SqlQuery78, mapping = {"myDataSource":raw_data_product_data_node1732404430681}, transformation_ctx = "SQLQuery_node1732406677413")

# Script generated for node Amazon S3
AmazonS3_node1732407239366 = glueContext.getSink(path="s3://dimensiontablesv1/dimension_order/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732407239366")
AmazonS3_node1732407239366.setCatalogInfo(catalogDatabase="datawarehousing",catalogTableName="OrderDim")
AmazonS3_node1732407239366.setFormat("glueparquet", compression="snappy")
AmazonS3_node1732407239366.writeFrame(SQLQuery_node1732406862433)
# Script generated for node Amazon S3
AmazonS3_node1732404038621 = glueContext.getSink(path="s3://dimensiontablesv1/dimension_customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732404038621")
AmazonS3_node1732404038621.setCatalogInfo(catalogDatabase="datawarehousing",catalogTableName="CustomerDim")
AmazonS3_node1732404038621.setFormat("glueparquet", compression="snappy")
AmazonS3_node1732404038621.writeFrame(SQLQuery_node1732403451704)
# Script generated for node Amazon S3
AmazonS3_node1732407236494 = glueContext.getSink(path="s3://dimensiontablesv1/dimension_delivery/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732407236494")
AmazonS3_node1732407236494.setCatalogInfo(catalogDatabase="datawarehousing",catalogTableName="DeliveryDim")
AmazonS3_node1732407236494.setFormat("glueparquet", compression="snappy")
AmazonS3_node1732407236494.writeFrame(SQLQuery_node1732406736707)
# Script generated for node Amazon S3
AmazonS3_node1732407232096 = glueContext.getSink(path="s3://dimensiontablesv1/dimension_product/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732407232096")
AmazonS3_node1732407232096.setCatalogInfo(catalogDatabase="datawarehousing",catalogTableName="ProductDim")
AmazonS3_node1732407232096.setFormat("glueparquet", compression="snappy")
AmazonS3_node1732407232096.writeFrame(SQLQuery_node1732406677413)
job.commit()