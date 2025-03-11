import logging
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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
