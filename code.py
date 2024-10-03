from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, split, lit, col, sum as spark_sum, monotonically_increasing_id, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("OlympicCountryDataPipeline").getOrCreate()

# Define schemas
olympic_schema = StructType([
    StructField("Country_Code", StringType(), True),
    StructField("Gold", IntegerType(), True),
    StructField("Silver", IntegerType(), True),
    StructField("Bronze", IntegerType(), True),
    StructField("Total", IntegerType(), True)
])

countries_schema = StructType([
    StructField("Country", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Population", DoubleType(), True),
    StructField("Area_sq_mi", DoubleType(), True),
    StructField("Pop_Density_per_sq_mi", DoubleType(), True),
    StructField("Coastline_coast_area_ratio", DoubleType(), True),
    StructField("Net_migration", DoubleType(), True),
    StructField("Infant_mortality_per_1000_births", DoubleType(), True),
    StructField("GDP_per_capita", IntegerType(), True),
    StructField("Literacy_percent", DoubleType(), True),
    StructField("Phones_per_1000", DoubleType(), True),
    StructField("Arable_percent", DoubleType(), True),
    StructField("Crops_percent", DoubleType(), True),
    StructField("Other_percent", DoubleType(), True),
    StructField("Climate", DoubleType(), True),
    StructField("Birthrate", DoubleType(), True),
    StructField("Deathrate", DoubleType(), True),
    StructField("Agriculture", DoubleType(), True),
    StructField("Industry", DoubleType(), True),
    StructField("Service", DoubleType(), True)
])

# Read All Olympic csv data
olympic_df = spark.read.csv("datasets/olympics/*.csv", schema=olympic_schema)
olympic_df = olympic_df.withColumn("Year", split(input_file_name(), "/").getItem(-1).cast("string"))
olympic_df = olympic_df.withColumn("Year", split(col("Year"), " ").getItem(0))

# Read Countries data
countries_df = spark.read.csv("datasets/countries/countries of the world.csv", header=True, schema=countries_schema)
countries_df = countries_df.withColumnRenamed("Country", "Country_Name")

# Step 1: Normalize the data by applying a foreign key

# Create a unique identifier for each country
countries_df = countries_df.withColumn("Country_ID", monotonically_increasing_id())

# Create a mapping dataframe
country_mapping = countries_df.select("Country_ID", "Country_Name", "Country_Code")

# Add the Country_ID to the Olympic data
olympic_df = olympic_df.join(country_mapping, on="Country_Code", how="left")

# Now we have normalized data with Country_ID as the foreign key

# Step 2: Transform the 2 data objects via denormalization

# Join the Olympic data with the full country data
denormalized_df = olympic_df.join(countries_df, on="Country_ID", how="left")

# Select and rename columns as needed
final_df = denormalized_df.select(
    col("Country_ID"),
    col("Country_Name"),
    col("Country_Code"),
    col("Year"),
    col("Gold"),
    col("Silver"),
    col("Bronze"),
    col("Total"),
    col("Region"),
    col("Population"),
    col("Area_sq_mi"),
    col("Pop_Density_per_sq_mi"),
    col("GDP_per_capita"),
    # Add other columns as needed
)

# Write to Parquet (a queryable format)
final_df.write.mode("overwrite").parquet("olympic_countries_denormalized.parquet")

print("Data pipeline completed. Denormalized results saved in 'olympic_countries_denormalized.parquet'")

# Stop Spark session
spark.stop()