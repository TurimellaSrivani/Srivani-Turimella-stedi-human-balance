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

# Load step trainer landing data from Glue catalog
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/step_trainer_landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding_node1",
)

# Load customer trusted data from Glue catalog
CustomerTrusted_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/customer_trusted_data/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node2",
)

# Join step trainer and customer trusted data on serialNumber
JoinedData_step = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerTrusted_node2,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinedData_step",
)

# Write the filtered step trainer data to the trusted zone (S3)
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=JoinedData_step,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://stedi-project-bucket/step_trainer_trusted_data/", "partitionKeys": []},
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
