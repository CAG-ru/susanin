import pathlib
import numpy as np
import pandas as pd
import re
import requests
import os
import shutil
from zipfile import ZipFile, ZIP_DEFLATED
import logging
logging.basicConfig(level=logging.DEBUG)


def get_datain_set(url, fname, csv_names, path='./data/', replace=False):
    """
    Сбор необходимых датасетов из каталога ИНИД
    """
    print(fname)
    if os.path.isfile(pathlib.Path(f'{path}/{fname}.zip')) and replace:
        os.remove(pathlib.Path(f'{path}/{fname}.zip'))
    if not os.path.isfile(pathlib.Path(f'{path}/{fname}.zip')):
        logging.info('download standard')
        download_standard(url)
        logging.info('extract standard')
        extract_standard(csv_names)
        os.remove(pathlib.Path('/tmp/std_tmp.zip'))
        logging.info('archive to new file')
        for csv_name in csv_names:
            shutil.move(str(pathlib.Path(f'/tmp/{csv_name}.csv')),
                   str(pathlib.Path(f'{path}')))
        logging.info(f'dataset moved to {path}')


def download_standard(url):
    save_as = pathlib.Path('/tmp/std_tmp.zip')
    if not os.path.isfile(save_as):
        response = requests.get(url, allow_redirects=True)
        if response.status_code == 200:
            with open(save_as, 'wb') as f:
                f.write(response.content)
            return str(pathlib.Path(f'{save_as}'))
        else:
            return False
    else:
        return save_as


def extract_standard(csv_names):
    archive = pathlib.Path('/tmp/std_tmp.zip')
    with ZipFile(archive, 'r') as zipObject:
        for curr_file in zipObject.namelist():
            logging.debug(f'{curr_file} found in archive')
            if curr_file in (x + '.csv' for x in csv_names):
                zipObject.extract(curr_file, pathlib.Path('/tmp'))
                logging.info(f'{curr_file} extracted')

if __name__ == '__main__':
    url = 'https://ds1.data-in.ru/Aggregated_datasets/Rosstat%2BMinfin%2BConsultant%2B/Istoriya_izmenen_munitsipal_RF_OKTMO_i_nasel_punkti_185_25.11.21/Istoriya_izmenen_munitsipal_RF_OKTMO_i_nasel_punkti_185_25.11.21.zip?'
    fname = 'oktmo'
    csv_files = ['np', 'mun_obr', 'np_mun_mapping']
    get_datain_set(url, fname, csv_files)
