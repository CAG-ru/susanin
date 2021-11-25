import pathlib
import logging
import numpy as np
import pandas as pd
import os

from geonorm.geomatch import Geomatch
from .ovrazhki_magic import build_panels, join_panels, truncate_to, get_mode
from .loader import get_datain_set

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("test")

class Ovrazhki:
    def __init__(self, region=None):
        url = 'https://ds1.data-in.ru/Aggregated_datasets/Rosstat%2BMinfin%2BConsultant%2B/Istoriya_izmenen_munitsipal_RF_OKTMO_i_nasel_punkti_185_25.11.21/Istoriya_izmenen_munitsipal_RF_OKTMO_i_nasel_punkti_185_25.11.21.zip?'
        fname = 'oktmo'
        csv_files = ['np', 'mun_obr', 'np_mun_mapping']
        
        if not all(os.path.isfile(pathlib.Path(
                f'./data/{csv_file}.csv')) for csv_file in csv_files):
            get_datain_set(url, fname, csv_files)


        self.standard = pd.read_csv(
            pathlib.Path('.').absolute().joinpath('data/mun_obr.csv'), sep=';')
        self.mo_1 = pd.read_csv(
            pathlib.Path('.').absolute().joinpath(
                'data/np_mun_mapping.csv'), sep=';')[['hash1', 'guid']]
        self.mo_2 = pd.read_csv(
            pathlib.Path('.').absolute().joinpath(
                'data/np_mun_mapping.csv'), sep=';')[['hash2', 'guid']]
        self.settlements = pd.read_csv(
            pathlib.Path('.').absolute().joinpath('data/np.csv'), sep=';')
        self.set_region(region)
        self.standard = self.standard[
            self.standard['region'] == self.region].copy()
        self.matcher = Geomatch(self.standard, 
                                match_columns=[ 'mun_type',
                                                    'municipality'])


    def set_region(self, region):
        regions = self.standard.drop_duplicates(subset='region')
        if region in regions.region.to_list():
            self.region = region
        else:
            matcher = Geomatch(regions, 
                                      match_columns=['region'])
            norm_region = matcher({'region' : region})
            self.region = norm_region['region']
            logging.info(
                f'Региона "{region}" в базе нет. Продолжаю с "{norm_region["region"]}"')
            
    def calculate_similarity(self, row):
        cur_hash = row.hash
        last_hash = row.last_hash
        cur_lvl = row.lvl
        if cur_lvl == 1:
            cur_settle_ids = self.mo_1[self.mo_1['hash1'] == cur_hash].guid
        else:
            cur_settle_ids = self.mo_1[self.mo_2['hash2'] == cur_hash].guid
        last_settle_ids = self.mo_1[self.mo_1['hash1'] == last_hash].guid
        if len(last_settle_ids) == 0:
            last_settle_ids = self.mo_2[self.mo_2['hash2'] == last_hash].guid
        
        cur_settle_ids = set(list(cur_settle_ids)) 
        last_settle_ids = set(list(last_settle_ids)) 
        settle_changes = cur_settle_ids.symmetric_difference(last_settle_ids)
        
        row['territory_closeness'] = max(0,
            1 - np.divide(len(settle_changes), len(cur_settle_ids)))
        
        row['settlement_changes'] = ', '.join(
            self.settlements[
            self.settlements['guid'].isin(
            settle_changes)].settlement.to_list())
        
        return row
            

    def preprocess_panel(self, panel_df, mun_name, 
                         time_name, mun_type):
        renamed = panel_df.copy()
        
        renamed[time_name] = pd.to_datetime(renamed[
            time_name].astype(str))
        renamed = renamed[renamed[time_name] >= pd.to_datetime('2015')]
        
        renamed['municipality'] = renamed[mun_name]
        if mun_type:
            renamed['mun_type'] = renamed[mun_type]
        result = self.matcher(renamed)
        renamed['norm_municipality'] = result.municipality
        renamed['norm_mun_type'] = result.mun_type
        return renamed 
            
    def check_my_panel(self,
                    panel_df,
                    time_name='year', 
                    truncate_to_year = False,
                    truncate_to_month = False,
                    mun_name='municipality',
                    mun_type=None):
        
        truncate = 'date'
        if truncate_to_year:
            truncate = 'year'
        elif truncate_to_month:
            truncate = 'month'        
        df = truncate_to(self.standard, truncate).copy()
        
        pr_panel = self.preprocess_panel(panel_df, 
                                         mun_name, 
                                         time_name,
                                         mun_type)
        
        
        merged_df = pr_panel.merge(df, right_on=['municipality', 'mun_type'],
                left_on=['norm_municipality', 'norm_mun_type'],
                suffixes=['', '_standard'], how='left')
        
        merged_df.drop(columns=['norm_municipality'], inplace=True)
        merged_df.drop(columns=['norm_mun_type'], inplace=True)
        
        result = merged_df.groupby('municipality_standard').apply(
            lambda x: build_panels(x, time_name, mun_type)).reset_index(drop=True)
        
        result.drop(columns=['time_start', 'time_end', 'region_standard'],
                   inplace=True)
        
        result['territory_closeness'] = '0'
        result['settlement_changes'] = ''
        result = result.apply(lambda x: self.calculate_similarity(x), axis=1)
        
        result = result.groupby('hash').apply(join_panels).reset_index(drop=True)
        return result
        
if __name__ == '__main__':
    kita_load = pd.read_csv('../../kita_load.csv', sep=';')
    ovrazhki = Ovrazhki('Нижегородская обла')
    result = ovrazhki.check_my_panel(kita_load, truncate_to_year=True, mun_type='mun_type')
    result.to_csv('../../kita_load.csv', sep=';', index=False)
    
