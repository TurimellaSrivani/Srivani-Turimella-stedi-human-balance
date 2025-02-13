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

# Load step trainer trusted data from Glue catalog
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/step_trainer_trusted_data/"], "recurse": True},
    transformation_ctx="StepTrainerTrusted_node1",
)

# Load accelerometer trusted data from Glue catalog
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-project-bucket/accelerometer_trusted_data/"], "recurse": True},
    transformation_ctx="AccelerometerTrusted_node1",
)

# Join step trainer and accelerometer data on sensorReadingTime/timestamp
JoinedData_ml = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="JoinedData_ml",
)

# Write the final machine learning curated data to the curated zone (S3)
MachineLearningCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=JoinedData_ml,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://stedi-project-bucket/machine_learning_curated_data/", "partitionKeys": []},
    transformation_ctx="MachineLearningCurated_node2",
)

job.commit()
