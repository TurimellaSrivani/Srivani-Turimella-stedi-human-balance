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

# Load customer trusted data from Glue catalog
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/customer_trusted_data/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1",
)

# Load accelerometer trusted data from Glue catalog
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/accelerometer_trusted_data/"], "recurse": True},
    transformation_ctx="AccelerometerTrusted_node1",
)

# Join customer and accelerometer data on email/user
JoinedData_curated = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinedData_curated",
)

# Write the curated customer data to the curated zone (S3)
CustomerCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=JoinedData_curated,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://stedi-project-bucket/customer_curated_data/", "partitionKeys": []},
    transformation_ctx="CustomerCurated_node2",
)

job.commit()
