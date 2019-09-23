from pyspark.sql import SparkSession
import boto3


s3_access = "AKIATDZBANSB27IARQEW"
s3_secret = "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"

kafka_bootstrap = 'pkc-4kgmg.us-west-2.aws.confluent.cloud:9092'
kfus = 'SS224GR3GBRDQQ6G'
kfps = 'PnxLwLbwW/uAZO0+AHIRBRBtMhGQqlWCTxy0ruOSkbNjQAP1LNjZhuveKV8qRm1s'

kafka_jaas = ("org.apache.kafka.common.security.plain.PlainLoginModule required "
              "username=\"" + kfus + "\" password=\"" + kfps + "\";")



# c5.4xlarge 16 CPUs 32 GiB Ram
def start_spark_session():

    spark = (SparkSession.builder
             .appName('spark')
             .config("spark.executor.memory", "30g")
             .config("spark.executor.cores", "10")
             .config("spark.driver.memory", "30g")
             .getOrCreate())
    sc = spark.sparkContext

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret)
    return spark



def get_s3(src, dst):
    client = boto3.client('s3',
                          aws_access_key_id=s3_access,
                          aws_secret_access_key=s3_secret)
    client.download_file('enginestream', src, dst)

def list_s3():
    client = boto3.client('s3',
                          aws_access_key_id=s3_access,
                          aws_secret_access_key=s3_secret)
    return [i["Key"] for i in client.list_objects_v2(Bucket="enginestream", Prefix="configs/")['Contents']]

