import pathlib
import logging
import numpy as np
import pandas as pd
import os

from geonorm.geomatch import Geomatch
from .ovrazhki_magic import build_panels, join_panels, truncate_to, get_mode, split_before_2015, expand_region
from .loader import prepare_data, read_config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Ovrazhki:
    def __init__(self, region=None, config_path=None):

        if not config_path:
            self.config = read_config()
        else:
            self.config = read_config(config_path)

        standard, mo_1, mo_2, settlements = prepare_data(
                self.config)

        
        self.standard = standard 
        self.mo_1 = mo_1
        self.mo_2 = mo_2
        self.settlements = settlements

        self.set_region(region)

        self.standard = self.standard[
            self.standard['region'] == self.region].copy()

        self.matcher = Geomatch(self.standard, 
                                match_columns=['municipality',
                                               'mun_type'])

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
            
    def calculate_similarity_row(self, row):
        settle_changes, closeness = self.calculate_similarity(
                row.hash, row.last_hash)
        
        row['territory_closeness'] = closeness

        row['settlement_changes'] = ', '.join(
            settle_changes)
        
        return row
           
    def find_settlements_by_hash(self, _hash):
        settle_ids = self.mo_1[
                self.mo_1['hash1'] == _hash].guid
        if len(settle_ids) == 0:
            settle_ids = self.mo_2[
                    self.mo_2['hash2'] == _hash].guid
        
        settles = self.settlements[
            self.settlements['guid'].isin(
            settle_ids)].settlement.to_list()

        return settles


    def calculate_similarity(self, first_hash, second_hash):
        first_settles = set(self.find_settlements_by_hash(
                first_hash))
        
        second_settles = set(self.find_settlements_by_hash(
                second_hash))

        settle_changes = first_settles.symmetric_difference(
                second_settles)

        return settle_changes, max(0, 1 - np.divide(
            len(settle_changes), len(first_settles)))
       


    def get_municipalities_by_id(self, mun_id):
        return self.standard[self.standard['id'] 
                == mun_id].copy()

    def preprocess_panel(self, panel_df, mun_name, 
                         time_name, mun_type):
        renamed = panel_df.copy()
        
        renamed[time_name] = pd.to_datetime(renamed[
            time_name].astype(str))
        
        renamed['municipality'] = renamed[mun_name]
        if mun_type:
            renamed['mun_type'] = renamed[mun_type]
            renamed = expand_region(renamed)
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
        
        before_2015, after_2015 = split_before_2015(pr_panel, time_name)
        merged_df = after_2015.merge(df, right_on=['municipality', 'mun_type'],
                left_on=['norm_municipality', 'norm_mun_type'],
                suffixes=['', '_standard'], how='left')
        
        
        result = merged_df.groupby('municipality_standard').apply(
            lambda x: build_panels(x, time_name, mun_type)).reset_index(drop=True)
        
        result.drop(columns=['time_start', 'time_end'],
                   inplace=True)
        
        result['territory_closeness'] = '0'
        result['settlement_changes'] = ''
        result = result.apply(lambda x: self.calculate_similarity_row(x), axis=1)
        
        result = result.groupby('hash').apply(join_panels).reset_index(drop=True)

        result = pd.concat([result, before_2015])
        result.drop(columns=['norm_municipality'], inplace=True)
        result.drop(columns=['norm_mun_type'], inplace=True)

        return result
        
if __name__ == '__main__':
    kita_load = pd.read_csv('../../kita_load.csv', sep=';')
    ovrazhki = Ovrazhki('Нижегородская обла')
    result = ovrazhki.check_my_panel(kita_load, truncate_to_year=True, mun_type='mun_type')
    result.to_csv('../../kita_load.csv', sep=';', index=False)
    
