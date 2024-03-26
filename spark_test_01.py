
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("My Spark Application") \
    .getOrCreate()

# spark.sparkContext.setLogLevel(".")
# Your PySpark code here
print("spark session is created successfully")

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

df.withColumn("Age", df["Age"])

# Stop the SparkSession
spark.stop()
