import sys
import requests

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import explode, col

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


def get_data():
    # Initialize Spark session
    spark = SparkSession.builder.appName("JSONtoDataFrame").getOrCreate()

    # API URL and parameters
    url = "https://odds.p.rapidapi.com/v4/sports/upcoming/odds"
    querystring = {"regions": "us", "oddsFormat": "decimal", "markets": "h2h,spreads", "dateFormat": "iso"}
    headers = {
        "x-rapidapi-key": "2042597c49msh48c7d7e021502ccp1e7bdcjsn91011bd9263b",
        "x-rapidapi-host": "odds.p.rapidapi.com"
    }

    # Make the API request and get the response
    response = requests.get(url, headers=headers, params=querystring)
    json_data = response.json()

    # Convert the JSON data to RDD and then to DataFrame
    rdd = spark.sparkContext.parallelize([Row(**item) for item in json_data])
    raw_df = spark.createDataFrame(rdd)

    # Print schema to check the structure of 'bookmakers' and 'markets'
    raw_df.printSchema()

    # Explode the 'bookmakers' array
    exploded_bookmakers_df = raw_df.withColumn("exploded_bookmakers", explode(col("bookmakers")))

    # Print schema to ensure 'exploded_bookmakers' contains the correct data type for 'markets'
    exploded_bookmakers_df.printSchema()

    # Explode the 'markets' array inside 'exploded_bookmakers'
    exploded_markets_df = exploded_bookmakers_df.withColumn("exploded_markets", explode(col("exploded_bookmakers.markets")))

    # Explode the 'outcomes' array inside 'markets'
    exploded_outcomes_df = exploded_markets_df.withColumn("exploded_outcomes", explode(col("exploded_markets.outcomes")))

    # Access individual fields within the exploded 'bookmakers', 'markets', and 'outcomes'
    final_df = exploded_outcomes_df.select(
        "id",
        "sport_title",
        "commence_time",
        "home_team",
        "away_team",
        col("exploded_bookmakers.key").alias("bookmaker_name"),
        col("exploded_bookmakers.title").alias("bookmaker_title"),
        col("exploded_markets.key").alias("market_type"),
        col("exploded_outcomes.name").alias("outcome_name"),
        col("exploded_outcomes.price").alias("outcome_price")
    )

    # Show the final DataFrame
    final_df.show(truncate=False)

# Call the function
get_data()
    
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
get_data()
job.commit()
