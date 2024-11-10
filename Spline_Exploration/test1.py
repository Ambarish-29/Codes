from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("MySparkJob2")\
.getOrCreate()

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


nicknames.write.mode("overwrite").csv("new1")