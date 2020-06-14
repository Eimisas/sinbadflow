def get_spark():
    '''Gets spark session from within the Databricks'''
    user_ns = get_ipython().user_ns
    if "spark" in user_ns:
        return user_ns["spark"]
    else:
        from pyspark.sql import SparkSession
        user_ns["spark"] = SparkSession.builder.getOrCreate()
        return user_ns["spark"]


def get_dbutils(spark):
    '''Gets dbutils from within the Databricks'''
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark.sparkContext)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils
