from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("QueryDenormalizedData").getOrCreate()

df = spark.read.parquet("olympic_countries_denormalized.parquet")

# Example query: Get the top 5 countries by total medals, including their population
top_5_countries = df.groupBy("Country_Name", "Population") \
                    .agg(spark_sum("Total").alias("Total_Medals")) \
                    .orderBy("Total_Medals", ascending=False) \
                    .limit(5)

top_5_countries.show()

spark.stop()