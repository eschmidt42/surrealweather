from six.moves import urllib
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from zipfile import ZipFile
import logging
from sqlalchemy import create_engine
import shutil
import json

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class Extractor:
    """Identifies URLs to zip and text files on the DWD's page and downloads them.
    """

    base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/annual/kl/historical/"
    extraction_dir = "../data/climate_annual_kl_historical/extracted"
    dry = False
    t_wait = .1  # [s]
    download_limit = None

    def __init__(self, base_url=None, extraction_dir=None, dry=False, t_wait=1., download_limit=None):

        if base_url is not None:
            self.base_url = base_url
        if extraction_dir is not None:
            self.extraction_dir = extraction_dir

        self.dry = dry
        self.t_wait = t_wait
        self.download_limit = download_limit

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

            if self.download_limit is not None and i >= self.download_limit:
                break

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


# defining how to read dwd files and how to store them in MySQL tables
table_specs = dict() # table name: {file name pattern: lambda, schema: dict, data frame conversions: dict}

table_specs["measurements_annual"] = {
    "file_name_pattern": lambda x: x.startswith("produkt_klima_jahr"),
    "dir_name_pattern": lambda x: x.startswith("jahreswerte_KL"),
    "schema": {
        "STATIONS_ID": str,
        "MESS_DATUM_BEGINN": str,
        "MESS_DATUM_ENDE": str,
        "QN_4": int,
        "QN_6": int,
        "eor": str,
        "JA_N": float,
        "JA_TT": float,
        "JA_TX": float,
        "JA_TN": float,
        "JA_FK": float,
        "JA_SD_S": float,
        "JA_MX_FX": float,
        "JA_MX_TX": float,
        "JA_MX_TN": float,
        "JA_PR": float,
        "JA_MX_RS": float
    },
    "df_conversions": {
        "MESS_DATUM_BEGINN": pd.to_datetime,
        "MESS_DATUM_ENDE": pd.to_datetime
    },
    "rename": {
        "MESS_DATUM_BEGINN": "begin_of_measurements",
        "MESS_DATUM_ENDE": "end_of_measurements"
    }
}

table_specs["stations_annual"] = {
    "file_name_pattern": lambda x: x.startswith("KL_Jahreswerte_Beschreibung_Stationen"),
    "schema": {
        "Stations_id": str,
        "von_datum": str,
        "bis_datum": str,
        "Stationshoehe": int,
        "geoBreite": float,
        "geoLaenge": float,
        "Stationsname": str,
        "Bundesland": str
    },
    "df_conversions": {
        "von_datum": pd.to_datetime,
        "bis_datum": pd.to_datetime
    },
    "rename": {
        "Stations_id": "STATIONS_ID",
        "von_datum": "begin_of_measurements",
        "bis_datum": "end_of_measurements"
    }
}


class Transformator:
    """Unpacks zip files and extracts files matching the provided name pattern.
    """

    extraction_dir = "../data/climate_annual_kl_historical/extracted"
    transformation_dir = "../data/climate_annual_kl_historical/transformed"
    dry = False
    target_table = "measurements_annual"
    #patterns = {"measurements": lambda x: x.startswith("produkt_klima_jahr")}  # pattern of the file to extract from zip

    def __init__(self, extraction_dir=None, transformation_dir=None, dry=False, target_table=None):

        if extraction_dir is not None:
            self.extraction_dir = extraction_dir
        if transformation_dir is not None:
            self.transformation_dir = transformation_dir

        self.dry = dry

        # if isinstance(patterns, dict) and all([callable(pattern) for pattern in patterns.values()]):
        #     self.patterns = patterns
        if target_table is not None:
            self.target_table = target_table

    def extract_zip_files(self, zip_file):

        dir_name = zip_file[:-4]
        source = os.path.join(self.extraction_dir, zip_file)

        if not self.dry:
            os.makedirs(self.transformation_dir, exist_ok=True)

        destination = os.path.join(self.transformation_dir, dir_name)

        logger.debug("Extracting {} to {}".format(source, destination))

        if not self.dry:
            with ZipFile(source, "r") as _zip:
                files = [m for m in _zip.namelist() if table_specs[self.target_table]["file_name_pattern"](m)]
                logger.debug("pattern matching files: {}".format(files))

                for i, _file in enumerate(files):
                    logger.debug("\t{}".format(_file))
                    _zip.extract(_file, path=destination)

    def clean(self, files):

        logger.debug("Removing {} files".format(len(files)))

        if not self.dry:
            for _file in files:
                os.remove(os.path.join(self.extraction_dir, _file))

        if self.extraction_dir != self.transformation_dir:
            logger.debug("Removing {}".format(self.extraction_dir))
            if not self.dry:
                os.rmdir(self.extraction_dir)

    def run(self, clean=False):

        files = [_file for _file in os.listdir(self.extraction_dir)
                 if os.path.isfile(os.path.join(self.extraction_dir, _file))]
        logger.debug("Found the following files in the extraction dir: {}".format(", ".join(files[:5])))

        zip_files = [_file for _file in files if _file.endswith(".zip")]
        txt_files = [_file for _file in files if _file.endswith(".txt")]
        logger.debug("Identified {} zip files and {} txt files".format(len(zip_files), len(txt_files)))

        # collecting contents from zip files
        for i, _file in enumerate(zip_files):
            if i % 100 == 0:
                logger.info("Processing zip file {}/{}".format(i + 1, len(zip_files)))
            elif i == len(zip_files) - 1:
                logger.info("Processing zip file {}/{}".format(i + 1, len(zip_files)))
            self.extract_zip_files(_file)

        # collecting not zipped text files
        for i, _file in enumerate(txt_files):
            if i % 100 == 0:
                logger.info("Processing txt file {}/{}".format(i + 1, len(txt_files)))
            elif i == len(zip_files) - 1:
                logger.info("Processing txt file {}/{}".format(i + 1, len(txt_files)))

            source = os.path.join(self.extraction_dir, _file)
            destination = os.path.join(self.transformation_dir, _file)
            logger.debug("Copying file from {} to {}".format(source, destination))

            if not self.dry:
                shutil.copyfile(source, destination)

        if clean:
            self.clean(zip_files + txt_files)


class Loader:

    transformation_dir = None
    db_creds_path = "../db_creds.json"

    tmp_table = "tmp"
    target_table = "measurements_annual"
    dataset = "surreal_weather"
    dry = False

    q_insert = """
insert into {dataset}.{target} ({target_cols})
select
  {source_cols}
from
  {dataset}.{source} as s
left join 
 {dataset}.{target} as t on s.{id} = t.{id} and s.{begin} = t.{begin} and s.{end} = t.{end}
where
  t.{id} is null and t.{begin} is null and t.{end} is null
"""

    def __init__(self, transformation_dir=None, db_creds_path=None, tmp_table=None,
                 target_table=None, dataset=None, dry=False):

        if transformation_dir is not None:
            self.transformation_dir = transformation_dir

        if db_creds_path is not None:
            self.db_creds_path = db_creds_path

        if tmp_table is not None:
            self.tmp_table = tmp_table

        if target_table is not None:
            assert target_table in table_specs, "provided target_table = '{}' is not one of the known tables: {}".format(target_table, table_specs.keys())
            self.target_table = target_table

        if dataset is not None:
            self.dataset = dataset

        self.dry = dry

        assert os.path.isfile(self.db_creds_path)

    def get_data_frame(self, _file):

        try:
            df = pd.read_csv(os.path.join(self.transformation_dir, _file), sep=";",
                             dtype=table_specs[self.target_table]["schema"])
        except UnicodeDecodeError:
            df = None
            with open(os.path.join(self.transformation_dir, _file), "r") as f:
                n = 0
                for i, line in enumerate(f):

                    if i == 0:
                        cols = list(filter(None, line.rstrip().split(" ")))
                        schema = table_specs[self.target_table]["schema"]
                        df = pd.DataFrame(columns=cols)
                    if i > 1:
                        vals = list(filter(None, line.rstrip().split(" ")))
                        # freaking spaces inside of values - assume this is because of the station names
                        if len(vals) > len(df.columns):
                            vals = vals[:6] + [" ".join(vals[6:-1])] + [vals[-1]]

                        df.loc[n] = vals
                        n += 1

                for col, dtype in table_specs[self.target_table]["schema"].items():
                    df[col] = df[col].astype(dtype)

        for col, fun in table_specs[self.target_table]["df_conversions"].items():
            df[col] = df[col].apply(fun)

        df = df.rename(columns=table_specs[self.target_table]["rename"])

        join_on_cols = {"id": "STATIONS_ID", "begin": "MESS_DATUM_BEGINN", "end": "MESS_DATUM_ENDE"}
        # updating join on columns with potentially new names assinged in the renaming property of the target table
        for key, val in join_on_cols.items():
            for old, new in table_specs[self.target_table]["rename"].items():
                if old != new and val == old:
                    join_on_cols[key] = new

        target_cols = ", ".join(df.columns.values)
        source_cols = "s." + ", s.".join(df.columns.values)

        return df, join_on_cols, source_cols, target_cols

    def write_to_mysql(self, files, db_creds):

        s = "mysql+mysqlconnector://{user}:{pw}@localhost/{db}".format(**db_creds)

        engine = create_engine(s, echo=False) if not self.dry else None

        for i, _file in enumerate(files):

            if i % 100 == 0:
                logger.info("Processing file {}/{}".format(i + 1, len(files)))

            logger.debug("Processing: {}".format(_file))

            if not self.dry:

                df, join_on_cols, source_cols, target_cols = self.get_data_frame(_file)

                target_exists = engine.dialect.has_table(engine, self.target_table)
                source_exists = engine.dialect.has_table(engine, self.tmp_table)

                if not target_exists:  # target table not existent yet --> create from scratch with the temporary table
                    logger.debug("Creating table {}.{}".format(self.dataset, self.target_table))
                    df.to_sql(self.target_table, con=engine)

                else:  # target table exists already --> update using the the temporary table
                    logger.debug("Updating table {dataset}.{target} with {dataset}.{tmp}".format(dataset=self.dataset,
                                                                                                 tmp=self.tmp_table,
                                                                                                 target=self.target_table))
                    q_drop = "drop table {dataset}.{source}".format(dataset=self.dataset, source=self.tmp_table)
                    with engine.begin() as conn:

                        if source_exists:
                            conn.execute(q_drop)

                        df.to_sql(self.tmp_table, con=engine)

                        # insert new values into target table
                        q_insert = self.q_insert.format(target=self.target_table, source=self.tmp_table,
                                                        target_cols=target_cols, source_cols=source_cols,
                                                        id=join_on_cols["id"], begin=join_on_cols["begin"],
                                                        end=join_on_cols["end"], dataset=self.dataset)

                        conn.execute(q_insert)

                        # remove temporary table
                        conn.execute(q_drop)

    def clean(self, files, dirs):

        logger.debug("Removing {} files and {} dirs".format(len(files), len(dirs)))

        if not self.dry:
            for _file in files:
                os.remove(os.path.join(self.transformation_dir, _file))

            for _dir in dirs:
                os.rmdir(os.path.join(self.transformation_dir, _dir))

    def run(self, clean=False):

        if "dir_name_pattern" in table_specs[self.target_table]:

            dirs = [_dir for _dir in os.listdir(self.transformation_dir) if
                     os.path.isdir(os.path.join(self.transformation_dir, _dir))
                     and table_specs[self.target_table]["dir_name_pattern"](_dir)]
        else:
            dirs = [""]

        files = [[os.path.join(_dir, _file) for _file in os.listdir(os.path.join(self.transformation_dir, _dir)) if
                 os.path.isfile(os.path.join(self.transformation_dir, _dir, _file))
                 and table_specs[self.target_table]["file_name_pattern"](_file)]
                 for _dir in dirs]
        files = [_file for _files in files for _file in _files]
        logger.debug("Reading {} files into MySQL".format(len(files)))

        with open(self.db_creds_path, "r") as f:
            db_creds = json.load(f)

        db_creds["db"] = self.dataset
        # user="root", pw="Mol9WPRntHAPNrNfjEhP", db="surreal_weather"
        self.write_to_mysql(files, db_creds)

        if clean:
            self.clean(dirs, files)


def main(do_extraction=False, do_transformation=False, do_loading=False, dry=True,
         base_url="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/annual/kl/historical/",
         extraction_dir="../data/climate_annual_kl_historical/extracted",
         transformation_dir="../data/climate_annual_kl_historical/transformed",
         clean=False, db_creds_path="../db_creds.json", tmp_table="tmp",
         target_table="measurements_annual", dataset="surreal_weather", t_wait=.1, download_limit=None):

    # Extract
    if do_extraction:
        extraction = Extractor(base_url=base_url, extraction_dir=extraction_dir, dry=dry, t_wait=t_wait,
                               download_limit=download_limit)
        extraction.run()

    # Transform
    if do_transformation:
        transformation = Transformator(extraction_dir=extraction_dir, transformation_dir=transformation_dir, dry=dry,
                                       target_table=target_table)
        transformation.run(clean=clean)

    # Loading
    if do_loading:
        loading = Loader(transformation_dir=transformation_dir, db_creds_path=db_creds_path, tmp_table=tmp_table,
                         target_table=target_table, dataset=dataset, dry=dry)
        loading.run(clean=clean)


if __name__ == "__main__":

    logger.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)

    main(do_extraction=False,
         do_transformation=False,
         do_loading=True,
         dry=False,
         base_url="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/annual/kl/historical/",
         extraction_dir="../data/climate_annual_kl_historical/extracted",
         transformation_dir="../data/climate_annual_kl_historical/transformed",
         clean=False,
         t_wait=.1,
         download_limit=None,
         db_creds_path="../db_creds.json",
         tmp_table="tmp",
         target_table="measurements_annual",
         dataset="surreal_weather")

    main(do_loading=True,
         dry=False,
         transformation_dir="../data/climate_annual_kl_historical/transformed",
         clean=False,
         db_creds_path="../db_creds.json",
         tmp_table="tmp",
         target_table="stations_annual",
         dataset="surreal_weather")
