import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("HELLO FROM GLUE!")

# Load data from S3 (Uncomment this when ready to process actual data)
# datasource0 = glueContext.create_dynamic_frame.from_options(
#     connection_type="s3", 
#     connection_options={"paths": ["s3://data-ingestion-sample-project/data/"]}, 
#     format="json"
# )

# Example transformation (add your logic)
# datasource1 = datasource0.select_fields(['field1', 'field2'])

# Write the transformed data back to S3 (Uncomment this when ready)
# datasink = glueContext.write_dynamic_frame.from_options(
#     frame=datasource1,
#     connection_type="s3",
#     connection_options={"paths": "s3://data-ingestion-sample-project/discovery/"},
#     format="json"
# )

# Commit the Glue job
job.commit()
