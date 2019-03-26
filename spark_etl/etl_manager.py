#!usr/bin/env python
"""
Entry point for setting up database,
pull data file from remote location
"""
import os
import sys
import zipfile

import logging
sys.path.insert(0, os.getcwd())
from etl_conf import conf
from data_manager.file_handler import FileHandler
from data_manager.database_handler import PgDb

# init logger
file_handler = logging.FileHandler(filename='logs/etl.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(filename)s:%(lineno)d] '
                           '%(levelname)s - %(message)s',
                    handlers=handlers)

logging.getLogger(__name__)

class EtlManager(object):
    def __init__(self, configs):
        self.configs = configs
        self.file_handler = FileHandler(self.configs)
        self.data_base = PgDb(self.configs)

    def check_create_database(self):
        logging.info("Initialised database check/ creation")
        pass

    def unzip_file(self, file_name):
        """
        extract file from archive
        :param file_path: full path to local folder/ file
        :return: list of files in archive
        """
        logging.info("Unzipping file from: {0}".format(file_name))
        if os.path.isfile(file_name):
            if zipfile.is_zipfile(file_name):
                dirname = os.path.splitext(file_name)[0]
                if not os.path.exists(dirname):
                    os.mkdir(dirname)
                with zipfile.ZipFile(file_name, "r") as zip_ref:
                    zip_ref.extractall(dirname)
                return dirname
            else:
                logging.warning("Invalid or bad ZIP file")
        else:
            logging.warning("Invalid file format")

    def pull_remote_data(self):
        logging.info("Downloading file from {0}".format(self.configs.d_set_1_url))
        file_location = self.file_handler.download_file(self.configs.d_set_1_url)
        self.unzip_file(file_location)

        logging.info("Downloading file {0}".format(self.configs.d_set_2_url))
        file_location = self.file_handler.download_file(self.configs.d_set_2_url)
        self.unzip_file(file_location)


if __name__ == '__main__':
    ENV = os.getenv('ENV', 'DEV')
    conf = conf[ENV]                                                    # load dev / prod conf based on environment variable
    etl_obj = EtlManager(conf)
    etl_obj.pull_remote_data()

    etl_obj.check_create_database()
