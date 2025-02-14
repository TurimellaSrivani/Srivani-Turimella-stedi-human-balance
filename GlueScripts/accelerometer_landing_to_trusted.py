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

# Load accelerometer landing data
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/accelerometer_landing/"], "recurse": True},
    transformation_ctx="AccelerometerLanding_node1",
)

# Load customer trusted data
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lakehouse",
    table_name="customer_trusted_data",
    transformation_ctx="CustomerTrusted_node1",
)

# Join accelerometer data with trusted customers
JoinedData_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinedData_node2",
)

# Write to trusted zone with schema updates enabled
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=JoinedData_node2,
    database="stedi_lakehouse",
    table_name="accelerometer_trusted_data",
    transformation_ctx="AccelerometerTrusted_node3",
    additional_options={"updateBehavior": "UPDATE_IN_DATABASE"},
)

job.commit()

