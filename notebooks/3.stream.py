# Run with 'run_stream_py'

from utils import start_spark_session
import pyspark.sql.functions as F
from utils import kafka_bootstrap, kafka_jaas

spark = start_spark_session()
spark.sparkContext.setLogLevel("WARN")

q_load_df = spark.read.parquet('q99.parquet')

parsed = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafka_bootstrap)
          .option("subscribe", "engine")
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.mechanism" , "PLAIN")
          .option("kafka.sasl.jaas.config", kafka_jaas)
          .load())

query = (parsed
         .select(parsed.value.cast("string"), parsed.timestamp)
         .select(F.split('value', '[:,]').alias('cols'), 'timestamp')
         .select(F.expr("cols[0]").cast("float").alias('time'),
                 F.expr("cols[1]").cast("long").alias('cycle'),
                 F.expr("cols[2]").cast("long").alias('conf'),
                 F.expr("cols[3]").cast("long").alias('run'),
                 F.expr("cols[4]").cast("float").alias('x'),
                 F.expr("cols[5]").cast("float").alias('y'),
                 F.expr("cols[6]").cast("float").alias('z'),
                'timestamp')
         .drop("cols"))

query = (query
         .withWatermark("timestamp", "5 second")
         .groupBy(F.window('timestamp', "10 second", "5 second"),
                  'cycle', 'conf')
         .agg(F.min(query.time).alias('mint'),
              F.max(query.time).alias('maxt')))

query = (query
         .join(q_load_df, query.conf == q_load_df.conf)
         .select(query.cycle,
                 q_load_df.q99,
                 (query.maxt - query.mint).alias('duration_s'))
         .select('*', F.expr('duration_s > q99').alias('alarm'))
         #.where('alarm')
        )

output = (query
         .writeStream
         .outputMode("Append")
         .format("console")
         .start())

output.awaitTermination()