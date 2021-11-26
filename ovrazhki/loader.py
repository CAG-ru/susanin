import pathlib
import numpy as np
import pandas as pd
import re
import requests
import os
import shutil
import json
from zipfile import ZipFile, ZIP_DEFLATED
import logging
logging.basicConfig(level=logging.DEBUG)


location = pathlib.Path(__file__).parent.absolute()
data_path = location.joinpath('data')

def read_config(cf_path=
        location.joinpath('config.json')):
    with open(str(pathlib.Path(f'{cf_path}')), 'r') as f:
        config = json.load(f)
    return config

def prepare_data(config):
    os.makedirs(data_path, exist_ok = True)

    if not all(os.path.isfile(data_path.joinpath(
        f'{csv_file}.csv')) for csv_file 
            in config['data']['csv_files']):
        get_datain_set(config['data']['url'], 
                config['data']['fname'], 
                config['data']['csv_files'])

    standard = pd.read_csv(
        data_path.joinpath('mun_obr.csv'), sep=';')
    mo_1 = pd.read_csv(
        data_path.joinpath('np_mun_mapping.csv'), sep=';')[['hash1', 'guid']]
    mo_2 = pd.read_csv(
        pathlib.Path('.').absolute().joinpath(
            'data/np_mun_mapping.csv'), sep=';')[['hash2', 'guid']]
    settlements = pd.read_csv(
        pathlib.Path('.').absolute().joinpath(
            'data/np.csv'), sep=';')
    
    return standard, mo_1, mo_2, settlements


def get_datain_set(url, fname, csv_names, path=data_path, replace=False):
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
