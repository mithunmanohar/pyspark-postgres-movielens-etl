"""
Interface for file related operations
"""
import os
import io
import gzip
import glob
import urllib
import shutil
import logging
import requests
import os, re
import zipfile
from zipfile import ZipFile

logging.getLogger(__name__)

class FileHandler(object):
    def __init__(self, configs):
        self.configs = configs

    def _parse_url(self, url):
        """
        remove query parameters to return url
        :param url: url with query parameters
        :return: clean url
        """
        url = urllib.parse.unquote(url)
        url = url.split("?")
        return url[0]

    def download_file(self, url):
        """
        download files to local folder
        :param url: dowload url
        :return: local download folder
        """
        try:
            parsed_url = self._parse_url(url)
            local_filename = self.configs.download_folder + os.sep \
                             + parsed_url.split('/')[-1]
            logging.debug(
                'downloading files to %s folder' % local_filename)
            r = requests.get(url, stream=True)
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            logging.debug(
                'downloaded files to %s folder' % local_filename)
            return local_filename
        except Exception as e:
            print(e)
            logging.exception("Exeception in downloading file due to %s"
                              % e)

    def get_file_list(self, dirname):
        """
        iterates recursively through a folder and returns list of
        files in it
        :param folder: full path to the target folder with files
        :return: list of files
        """
        walker = os.walk(dirname)
        files_to_process = []
        for root, dirs, files in walker:
            if not root.startswith('__'):
                for f in files:
                    if f.startswith(".") or f.startswith('_') or \
                            f.startswith('__') and not f.endswith(".gz"):
                        pass
                    else:
                        input_file = root + os.sep + f
                        files_to_process.append(input_file)
            else:
                print("not processing", root)
        return files_to_process

    def unzip_file(self, file_name):
        """
        extract file from archive
        :param file_path: full path to local folder/ file
        :return: list of files in archive
        """
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