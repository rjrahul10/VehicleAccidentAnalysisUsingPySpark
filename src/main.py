from pyspark.sql import SparkSession
from datetime import datetime

from parser import Parser
from utilities import Utilities
from logger import Logger
from job import Job

if __name__ == '__main__':
    # parser parsing the command line arguments
    parser = Parser().getParser()
    args = parser.parse_args()
    yaml_file = args.config

    util = Utilities()
    cfg = util.read_config(yaml_file)
    logfile = cfg.get('log_file')

    logger = Logger(logfile)
 
    if args.appname is None:
        appname = 'Spark'
    else:
        appname = args.appname
    spark = SparkSession.builder.appName(appname).getOrCreate()
    logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + "  Spark session started")

    runner = Job(spark, cfg, logger)
    logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Data Analysis startecd")
    runner.run()
    logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Data Analysis completed")
    spark.stop()
    logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Spark Session stopped ")
