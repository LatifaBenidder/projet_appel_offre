import pyspark
import pandas as pd
from pyspark.sql.functions import regexp_extract
from pyspark.sql import SparkSession
inputUri="gs://input_bucket_project/data.csv"
outputUri="gs://input_bucket_project/data_cleaned.csv"
spark = SparkSession.builder.getOrCreate()
data=spark.read.csv(inputUri, header=True)
data = data.dropna()
data = data.withColumn("Remaining_times", regexp_extract("remaining_times", r"(\d+)", 1).cast("integer"))
data=data.toPandas()
data['deadline'] = pd.to_datetime(data['deadline'], format='%Y-%m-%d', errors='coerce')
data = data[['title', 'pays', 'deadline', 'lien', 'Remaining_times']]
data.to_csv(outputUri,index=False)  
# Arrêtez la session Spark
spark.stop()  