import logging
import yaml
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from typing import Dict, Any





def load_config_file(config_path: str) -> Dict[str, Any]:
    """Execute and load the configuration in the YAML file for the application. Eliminate the unnecessary need for hardcoding settings in the code and flexibility for modification and maintenance to the configuration."""
    try:
      with open(config_path, 'r') as config_file:
        return yaml.safe_load(config_file)
    """Use yaml.safe_load instead of yaml.load to prevent security risk such as arbitrary code from executing in the YAML file"""
    except Exception as e:
      raise RuntimeError(f"Failed to load the configuration specified: {str(e)}")
    """Handle the errors if attempts fail with useful output message. - Error Handling"""


def setup_logging(config: Dict[str, Any]) -> None:
    """Enable logging configuration. Do not use logging as Python module being used"""
    log_config = config.get("logging", {})
    log_level =  log_config.get("level", "INFO").upper()

    valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

    if log_level not in valid_levels:
        log_level = "INFO" 
    """Fallback if none of the above is valid. Thereby using INFO as fault"""

    logging.basicConfig(
        level=getattr(logging, log_level,
        format=log_config.get("format", "%(asctime)/s -%(levelname)s - %(message)s"),
        filename=log_config.get("file")
    )

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Initialize and configure the Spark session based on configuration"""
    logger = logging.getLogger(__name__)
    logger.info("Initializing Spark session")
    
    spark_config = config.get("spark", {})
    app_name = spark_config.get("app_name", "KafkaStreamTest")
    packages = ",".join(spark_config.get("packages", []))
    
    builder = SparkSession.builder.appName(app_name)

    if packages:
        builder = builder.config("spark.jars.packages", packages)
    
    for key, value in spark_config.get("conf", {}).items():
        builder = builder.config(key, value)

    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            spark = builder.getOrCreate()
            logger.info("✅ Spark session successfully created")
            break
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session (Attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                raise RuntimeError("🚨 Spark session initialization failed after multiple attempts")
            time.sleep(retry_delay)  # Wait before retrying

    # 📌 Cassandra Configurations
    cassandra_config = config.get("cassandra", {})
    cassandra_host = cassandra_config.get("host")
    cassandra_port = cassandra_config.get("port")

    if not cassandra_host or not cassandra_port:
        raise ValueError("🚨 Cassandra host/port must be specified in the configuration!")

    spark.conf.set("spark.cassandra.connection.host", cassandra_host)
    spark.conf.set("spark.cassandra.connection.port", cassandra_port)

    logger.info(f"✅ Connected to Cassandra at {cassandra_host}:{cassandra_port}")

    return spark
