#!usr/bin/env python

"""
Config manager for etl

"""

import logging
import os

logging.getLogger(__name__)

class Config(object):
    pass

class DevelopmentConfig(Config):
    DEBUG = True
    AWS_REGION = "us-east-1"

    download_folder = os.getcwd() + os.sep + "data"

    LOG_LEVEL = logging.INFO
    LOGS_ROOT = "/logs"

    pg_db = {"pg_data_lake": {"dbname": "datalake",
                              "user": "datalake",
                              "host": "xxxxxxxxxxx.us-east-2.rds.amazonaws.com",
                              "password": "xxxxx",
                              "port": "5432"

                                 }}
    d_set_1_url = "http://files.grouplens.org/datasets/movielens/ml-20m-youtube.zip"
    d_set_2_url = "http://files.grouplens.org/datasets/movielens/ml-10m.zip"



class ProductionConfig(Config):
    pass


class TestingConfig(Config):
    pass

conf = {
    'DEV': DevelopmentConfig,
    'PROD': ProductionConfig,
    'TEST': TestingConfig
}
