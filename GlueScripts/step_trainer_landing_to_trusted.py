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

# Load step trainer landing data
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/step_trainer_landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding_node1",
)

# Load customer trusted data
CustomerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_lakehouse",
    table_name="customer_trusted_data",
    transformation_ctx="CustomerTrusted_node2",
)

# Join step trainer data with trusted customers
JoinedData_step = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerTrusted_node2,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinedData_step",
)

# Write to trusted zone with schema updates enabled
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=JoinedData_step,
    database="stedi_lakehouse",
    table_name="step_trainer_trusted_data",
    transformation_ctx="StepTrainerTrusted_node3",
    additional_options={"updateBehavior": "UPDATE_IN_DATABASE"},
)

job.commit()

