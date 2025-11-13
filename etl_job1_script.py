"""
ETL Job 1: ETL_RAWDATA_TO_DIMENSIONS
Transforms raw data from S3 into dimension tables
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =====================================================
# ORDER DIMENSION
# =====================================================

# Load raw order data from Data Catalog
raw_data_order_data_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="rawdata_order",
    transformation_ctx="raw_data_order_data_node"
)

# Transform with SQL Query
SqlQuery_order = '''
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

SQL_Query_order_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery_order,
    mapping={"myDataSource": raw_data_order_data_node},
    transformation_ctx="SQL_Query_order_node"
)

# Write to S3 as Parquet
Data_target_order_node = glueContext.write_dynamic_frame.from_options(
    frame=SQL_Query_order_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://dimensiontables/orderdim/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Data_target_order_node"
)

# =====================================================
# CUSTOMER DIMENSION
# =====================================================

# Load raw customer data from Data Catalog
raw_data_customer_data_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="rawdata_customer",
    transformation_ctx="raw_data_customer_data_node"
)

# Transform with SQL Query
SqlQuery_customer = '''
SELECT DISTINCT
    CUSTOMER_ID,
    CUSTOMER_FNAME,
    CUSTOMER_SEGMENT,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_COUNTRY
FROM myDataSource;
'''

SQL_Query_customer_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery_customer,
    mapping={"myDataSource": raw_data_customer_data_node},
    transformation_ctx="SQL_Query_customer_node"
)

# Write to S3 as Parquet
Data_target_customer_node = glueContext.write_dynamic_frame.from_options(
    frame=SQL_Query_customer_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://dimensiontables/customerdim/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Data_target_customer_node"
)

# =====================================================
# PRODUCT DIMENSION
# =====================================================

# Load raw product data from Data Catalog
raw_data_product_data_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="rawdata_product",
    transformation_ctx="raw_data_product_data_node"
)

# Transform with SQL Query
SqlQuery_product = '''
SELECT DISTINCT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY_ID,
    CATEGORY_NAME,
    PRODUCT_PRICE
FROM myDataSource;
'''

SQL_Query_product_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery_product,
    mapping={"myDataSource": raw_data_product_data_node},
    transformation_ctx="SQL_Query_product_node"
)

# Write to S3 as Parquet
Data_target_product_node = glueContext.write_dynamic_frame.from_options(
    frame=SQL_Query_product_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://dimensiontables/productdim/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Data_target_product_node"
)

# =====================================================
# DELIVERY DIMENSION
# =====================================================

# Load raw delivery data from Data Catalog
raw_data_delivery_data_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="rawdata_delivery",
    transformation_ctx="raw_data_delivery_data_node"
)

# Transform with SQL Query
SqlQuery_delivery = '''
SELECT DISTINCT
    ORDER_ID,
    SHIPPING_DATE,
    DELIVERY_STATUS,
    SHIPPING_MODE,
    SHIPPING_DAYS
FROM myDataSource;
'''

SQL_Query_delivery_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery_delivery,
    mapping={"myDataSource": raw_data_delivery_data_node},
    transformation_ctx="SQL_Query_delivery_node"
)

# Write to S3 as Parquet
Data_target_delivery_node = glueContext.write_dynamic_frame.from_options(
    frame=SQL_Query_delivery_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://dimensiontables/deliverydim/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Data_target_delivery_node"
)

# Commit job
job.commit()