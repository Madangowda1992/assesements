from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sales Data") \
    .config("spark.memory.offheap.enabled","true")\
    .config("spark.driver.memory","4g")\
    .getOrCreate()

# Local paths to the downloaded Excel files
local_file_a = "C:\\Users\\Deepak\\Downloads\\order_region_a.xlsx"
local_file_b = "C:\\Users\\Deepak\\Downloads\\order_region_a.xlsx"

# Reading Excel files into pandas DataFrames
df_region_a = pd.read_excel(local_file_a, engine='openpyxl')
df_region_b = pd.read_excel(local_file_b, engine='openpyxl')

# Convert pandas DataFrame to PySpark DataFrame
region_a_spark = spark.createDataFrame(df_region_a)
region_b_spark = spark.createDataFrame(df_region_b)



# Show schema and data in PySpark
# region_a_spark.printSchema()
# region_a_spark.show()

# region_b_spark.printSchema()
# region_b_spark.show()


# Add a column to identify the region (A or B)
region_a_spark = region_a_spark.withColumn("region", expr("'A'"))
region_b_spark = region_b_spark.withColumn("region", expr("'B'"))

#2)Transformations
# Combine the data from both regions
combined_df = region_a_spark.unionByName(region_b_spark)


# Add a total_sales column: total_sales = QuantityOrdered * ItemPrice
combined_df = combined_df.withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))

# Add a net_sale column: net_sale = total_sales - PromotionDiscount
combined_df = combined_df.withColumn("net_sale", col("total_sales") - col("PromotionDiscount"))
combined_df.show()


# Remove duplicates based on OrderId
combined_df1 = combined_df.dropDuplicates(["OrderId"])
combined_df1.show()



# Exclude orders where net_sale <= 0
filtered_df = combined_df1.filter(col("net_sale") > 0)

# Show transformed data (for validation)
filtered_df.limit(1).show()

