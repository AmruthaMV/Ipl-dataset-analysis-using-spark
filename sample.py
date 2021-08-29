from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
rawDF = spark.read.json("sample.json", multiLine = "true")
rawDF.printSchema()
sampleDF = rawDF.withColumnRenamed("id", "key")
batDF = sampleDF.select("key", "batters.batter")
batDF.printSchema()
batDF.show(1, False)
bat2DF = batDF.select("key", explode("batter").alias("new_batter"))
bat2DF.show()
bat2DF.printSchema()
bat2DF.select("key", "new_batter.*").show()
finalBatDF = (sampleDF
        .select("key",  
explode("batters.batter").alias("new_batter"))
        .select("key", "new_batter.*")
        .withColumnRenamed("id", "bat_id")
        .withColumnRenamed("type", "bat_type"))
finalBatDF.show()
topDF = (sampleDF
        .select("key", explode("topping").alias("new_topping"))
        .select("key", "new_topping.*")
        .withColumnRenamed("id", "top_id")
        .withColumnRenamed("type", "top_type")
        )
topDF.show(10, False)
