from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("product_category").getOrCreate()

data = [("product1", "category1"),
        ("product1", "category2"),
        ("product2", "category1"),
        ("product3", "category3"),
        ("product4", None)]

df = spark.createDataFrame(data, ["product", "category"])
pairs_df = df.filter(df["category"].isNotNull()).select("product", "category")

product_without_category_df = df.filter(df["category"].isNull()).select("product")

pairs_df.show()
product_without_category_df.show()