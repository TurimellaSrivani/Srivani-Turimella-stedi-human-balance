import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load customer landing data
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/customer_landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node1",
)

# Filter customers who agreed to share data
PrivacyFilter_node1693740592249 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (row["shareWithResearchAsOfDate"] is not None),
    transformation_ctx="PrivacyFilter_node1693740592249",
)

# Write to trusted zone with schema updates enabled
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=PrivacyFilter_node1693740592249,
    database="stedi_lakehouse",
    table_name="customer_trusted_data",
    transformation_ctx="CustomerTrusted_node3",
    additional_options={"updateBehavior": "UPDATE_IN_DATABASE"},
)

job.commit()

