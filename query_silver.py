# add to a quick debug_silver.py
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from ingestion.spark_config import get_spark_session

spark = get_spark_session("DebugSilver")
df = spark.read.format("delta").load("s3a://ghostkitchen-lakehouse/silver/orders/normalized")

df.printSchema()
df.groupBy("platform").count().show()
df.select("customer_key", "order_total_cents", "order_timestamp", 
          "kitchen_id", "brand_name", "platform", 
          "platform_order_id", "raw_email", "is_late_arriving").show(3, truncate=True)