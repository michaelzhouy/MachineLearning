from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# 初始化 spark 和配置（集群环境）
ss = SparkSession.builder \
    .config('hive.metastore.uris', 'thrift://10.21.3.10:9083') \
    .appName('app_name') \
    .enableHiveSupport() \
    .getOrCreate()

# generate data_frame
# select sql to spark df
df = ss.sql("SELECT organization_id, count(*) as cnt FROM isc.item_consume_prod GROUP BY organization_id")
df.show()