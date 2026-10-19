from pyspark.sql import SparkSession
from delta.tables import DeltaTable
spark = SparkSession.builder.appName("smoke_delta").getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS smoke")
spark.sql("DROP TABLE IF EXISTS smoke.t")
spark.sql("CREATE TABLE smoke.t (id INT, v STRING) USING DELTA LOCATION 's3a://silver/_smoke/t'")
spark.sql("INSERT INTO smoke.t VALUES (1,'a'),(2,'b')")
spark.createDataFrame([(2,'b2'),(3,'c')],["id","v"]).createOrReplaceTempView("src")
spark.sql("MERGE INTO smoke.t t USING src s ON t.id=s.id WHEN MATCHED THEN UPDATE SET v=s.v WHEN NOT MATCHED THEN INSERT *")
spark.sql("SELECT * FROM smoke.t ORDER BY id").show()
spark.sql("DESCRIBE HISTORY smoke.t").select("version","operation").show()
print("v0 rows:", spark.sql("SELECT * FROM smoke.t VERSION AS OF 1").count())
print("catalog tables:", [t.name for t in spark.catalog.listTables("smoke")])
spark.stop()
