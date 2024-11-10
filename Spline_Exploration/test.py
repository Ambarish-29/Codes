from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("MySparkJob2")\
.getOrCreate()

# Read user_favorites to DataFrame and create a temporary view
user_favorites = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("user_favorites.csv")
)

spark.sql("create database if not exists new_db")

#user_favorites.createOrReplaceTempView("user_favorites")
user_favorites.write.mode("overwrite").saveAsTable("new_db.test3")

# Read locations to DataFrame and create a temporary view
locations = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("locations.csv")
)
#locations.createOrReplaceTempView("locations")

locations.write.mode("overwrite").saveAsTable("new_db.test4")

# Join user_favorites and locations, and generate the nicknames
nicknames = spark.sql("""
SELECT
  a.id,
  CONCAT(
    favorite_color,
    ' ',
    state
  ) AS nickname
FROM new_db.test3 a
JOIN new_db.test4 b
ON a.favorite_city = b.city
""")

# Write output and print final DataFrame to console
nicknames.write.mode("overwrite").csv("new")
#nicknames.show(20, False)