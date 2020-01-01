from six.moves import urllib
from bs4 import BeautifulSoup
import pandas as pd
import re
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import time
from zipfile import ZipFile
import logging
import datetime
from sqlalchemy import create_engine
import shutil

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.setLevel(logging.DEBUG)
ch.setLevel(logging.DEBUG)


class Extractor:
    """Identifies URLs to zip and text files on the DWD's page and downloads them.
    """

    base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/annual/kl/historical/"
    extraction_dir = "../data/climate_annual_kl_historical/extracted"
    dry = False
    t_wait = .1  # [s]

    def __init__(self, base_url=None, extraction_dir=None, dry=False, t_wait=1.):

        if base_url is not None:
            self.base_url = base_url
        if extraction_dir is not None:
            self.extraction_dir = extraction_dir

        self.dry = dry
        self.t_wait = t_wait

    @staticmethod
    def get_html_soup(target_url, encoding="utf-8", dry=False):

        logger.debug("Parsing: {}".format(target_url))
        if not dry:
            r = urllib.request.urlopen(target_url)
            logger.debug("HTML code: {}".format(r.code))

            res = r.read().decode(encoding)
            soup = BeautifulSoup(res)
        else:
            soup = None

        return soup

    @staticmethod
    def download_single_file(url, path, dry=False):

        logger.debug("Downlading {} to {}".format(url, path))

        if not dry:
            r = urllib.request.urlopen(url)

            with open(path, "wb") as f:
                content = r.read()
                f.write(content)

    def get_all_files(self, files):

        logger.debug("Creating {} for local file storage".format(self.extraction_dir))
        if not self.dry:
            os.makedirs(self.extraction_dir, exist_ok=True)

        for i, _file in enumerate(files):

            if i % 100 == 0:
                logger.info("Processing file {}/{}".format(i + 1, len(files)))
            path = os.path.join(self.extraction_dir, _file)
            url = "{base}/{file}".format(base=self.base_url, file=_file)
            self.download_single_file(url, path, dry=self.dry)
            time.sleep(self.t_wait)

    def run(self):
        """Main function to perform URL search and storage of zip files.
        """
        soup = self.get_html_soup(self.base_url)

        files = []
        if not self.dry:
            files = [tag["href"] for tag in soup.find_all(
                lambda tag: tag.name == "a" and (tag["href"].endswith("zip") or tag["href"].endswith("txt")))]
        logger.debug("Found {} files".format(len(files)))

        self.get_all_files(files)