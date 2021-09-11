from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # define abspath and secrets path 3
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    # load application and secret files2afwfweg
    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    staging_dir = app_conf["s3_conf"]["staging_dir"]
    stg_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + staging_dir + "/" + "CP"

    spark = SparkSession \
        .builder \
        .appName("target data load") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()

    cp_df = spark\
        .read\
        .parquet(stg_path)\
        .filter(col("run_dt") == current_date())

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/uniliver/target_data_loading.py

