from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, avg

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sales Data ETL") \
    .config("spark.jars", "C:\\Users\\Deepak\\Desktop\\Project-pro\\my_venv\\jarfiles\\mysql-connector-j-8.4.0.jar") \
    .getOrCreate()

# MySQL connection properties
jdbc_url = "jdbc:mysql://127.0.0.1:3306/"
table_name = "sales_data"
connection_properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Creating the table 'sales_data' in MySQL
def create_sales_data_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales_data (
        OrderId VARCHAR(255),
        OrderItemId VARCHAR(255),
        QuantityOrdered INT,
        ItemPrice FLOAT,
        PromotionDiscount FLOAT,
        batch_id INT,
        region VARCHAR(10),
        total_sales FLOAT,
        net_sale FLOAT
    );
    """
 
    spark.sql(f"CREATE DATABASE IF NOT EXISTS Assesment_DB")
    spark.read.jdbc(url=jdbc_url, table=f"({create_table_query}) as tmp", properties=connection_properties)
    print("Table 'sales_data' created successfully.")

# Creating DataFrame with the transformed data (this is the data from transformation)
data = [
    ("171-0004135-1657958", "11168926867715", 1, 699.0, 0.0, 359, "A", 949.0, None),
    ("171-0001497-9165123", "19760289917699", 1, 699.0, 0.0, 1135, "A", 699.0, None),
    ("171-001127-1363587", "5949764090983", 1, 399.0, 0.0, 297, "A", 399.0, None),
    ("171-0004370-0601169", "57571368836739", 1, 399.0, 0.0, 764, "A", 1699.0, None),
    ("171-0004526-2828348", "33851287940183", 1, 899.0, 0.0, 764, "A", 1699.0, None),
    ("171-0006781-3853713", "43866103544491", 1, 399.0, 0.0, 809, "A", 399.0, None),
    ("171-0011547-3809807", "15941372805855", 1, 1399.0, 0.0, 15, "A", 1399.0, None),
    ("171-0003497-4305927", "15941372805855", 1, 1399.0, 0.0, 338, "A", 1399.0, None),
    ("171-0004767-8036635", "33529375931679", 1, 349.0, 0.0, 868, "A", 349.0, None),
    ("171-0003830-2254725", "31456280665443", 1, 499.0, 0.0, 332, "A", 499.0, None),
    ("171-0004978-9786935", "2915829310475", 1, 499.0, 0.0, 242, "A", 499.0, None),
    ("171-0011712-3788355", "22456000251587", 1, 2899.0, 0.0, 1301, "A", 2899.0, None),
    ("171-0010659-1412433", "45841722968915", 1, 2999.0, 0.0, 184, "A", 2899.0, None),
    ("171-0011765-7906794", "66263658588357", 1, 599.0, 0.0, 122, "A", 599.0, None),
    ("171-0012832-1769977", "4749521583307", 1, 676.0, 0.0, 676, "A", 676.0, None),
    ("171-0005769-5365797", "2131579938611", 1, 1499.0, 0.0, 869, "A", 1499.0, None),
    ("171-0011025-3017937", "18207271151803", 1, 1199.0, 0.0, 821, "A", 1199.0, None),
    ("171-0015668-5560953", "89108747369927", 1, 699.0, 0.0, 799, "A", 699.0, None),
    ("171-0014630-9547521", "3526651136027", 1, 499.0, 0.0, 325, "A", 499.0, None)
]


schema = ["OrderId", "OrderItemId", "QuantityOrdered", "ItemPrice", 
          "PromotionDiscount", "batch_id", "region", "total_sales", "net_sale"]

# Show the transformed data
df_transformed.show()

# Write a function to load the transformed data into the MySQL table 'sales_data'
def load_data_to_mysql(df, table_name):
    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
    print(f"Data loaded into MySQL table '{table_name}' successfully.")

# Load the transformed data into the 'sales_data' table
load_data_to_mysql(df_transformed, table_name)

# 1. Count the total number of records in the PySpark DataFrame
total_records = df_transformed.count()
print(f"Total Records: {total_records}")

# 2. Finding the total sales amount by region
total_sales_by_region = df_transformed.groupBy("region").agg(spark_sum("total_sales").alias("total_sales_amount"))
print("Total Sales by Region:")
total_sales_by_region.show()

# 3. Finding the average sales amount per transaction
avg_sales_per_transaction = df_transformed.agg(avg("total_sales").alias("avg_sales_per_transaction"))
print("Average Sales Amount per Transaction:")
avg_sales_per_transaction.show()

# Optional: Write SQL queries for validation if needed
# Query MySQL using Spark to ensure data has been inserted
def query_mysql(sql_query):
    query = f"({sql_query}) as tmp"
    df_result = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    df_result.show()

#  Count records in MySQL
query_mysql("SELECT COUNT(*) FROM sales_data")

#  Total sales by region
query_mysql("SELECT region, SUM(total_sales) as total_sales_amount FROM sales_data GROUP BY region")

#  Average sales amount per transaction
query_mysql("SELECT AVG(total_sales) as avg_sales_per_transaction FROM sales_data")
