"""
ETL Job 2: ETL_DIM_TO_FACT
Joins dimension tables to create the fact_sales table
"""

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

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =====================================================
# LOAD DIMENSION TABLES
# =====================================================

# Script generated for node productDim
productDim_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="productdim",
    transformation_ctx="productDim_node"
)

# Script generated for node deliveryDim
deliveryDim_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="deliverydim",
    transformation_ctx="deliveryDim_node"
)

# Script generated for node orderDim
orderDim_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="orderdim",
    transformation_ctx="orderDim_node"
)

# Script generated for node customerDim
customerDim_node = glueContext.create_dynamic_frame.from_catalog(
    database="datawarehousing",
    table_name="customerdim",
    transformation_ctx="customerDim_node"
)

# =====================================================
# CREATE FACT TABLE
# =====================================================

# Script generated for node SQL Query
SqlQuery_fact = '''
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
FROM orderDim o
INNER JOIN customerDim c ON o.ORDER_CUSTOMER_ID = c.CUSTOMER_ID
INNER JOIN productDim p ON o.ORDER_PRODUCT_ID = p.PRODUCT_ID
INNER JOIN deliveryDim d ON o.ORDER_ID = d.DELIVERY_ID
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
'''

SQL_Query_fact_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery_fact,
    mapping={
        "orderDim": orderDim_node,
        "customerDim": customerDim_node,
        "productDim": productDim_node,
        "deliveryDim": deliveryDim_node
    },
    transformation_ctx="SQL_Query_fact_node"
)

# =====================================================
# WRITE FACT TABLE TO S3
# =====================================================

# Script generated for node Amazon S3
AmazonS3_fact_node = glueContext.getSink(
    path="s3://factsalesv1/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_fact_node"
)
AmazonS3_fact_node.setCatalogInfo(catalogDatabase="datawarehousing", catalogTableName="FactSales")
AmazonS3_fact_node.setFormat("glueparquet", compression="snappy")
AmazonS3_fact_node.writeFrame(SQL_Query_fact_node)

# Commit job
job.commit()