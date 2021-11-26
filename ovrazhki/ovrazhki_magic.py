import pandas as pd
import re

# Fun to make with panel data

def build_panels(mun_df, time_name, mun_type=None):
    '''
    Функция, которая по датасету для конкретного муниципалитета
    выстраивает панель из дат измерений показателей и проставляет
    сообщения о возможных трудностях.
    На входе:
    mun_df – объект pandas.DataFrame, полученный в результате 
    группировки по названию муниципалитета данных мёрджа между 
    наблюдениями и справочником МО.
    time_name – string, название переменной, содержащей даты измерения.
    mun_type – название переменной, содержащей тип муниципалитета (может отсутствовать)
    На выходе:
    panel – объект pandas.DataFrame, 
    тот же датасет, что и mun_df, но без повторений по датам и с проставленными
    сообщениями в колонках message и last_hash.
    '''
    panel = mun_df.groupby(time_name).apply(
        lambda x:
        track_ovrazhki(
            x, time_name, mun_type)).reset_index(
                drop=True)
    panel = panel.sort_values(time_name)
    last_hash = None
    for index, row in panel.iterrows():
        if not last_hash:
            last_hash = row['hash']
            panel.at[index, 'last_hash'] = last_hash
        else:
            panel.at[index, 'last_hash'] = last_hash
            if last_hash != row['hash']:
                if (panel.at[index - 1, 
                        'message'].startswith(
                        'more')):
                    panel.at[index, 'message'] = 'ok'
                    panel.at[index, 'last_hash'] = row['hash']
                elif (row['message'] == 'ok'):
                    panel.at[index, 'message'] = 'new panel! see last hash'
        last_hash = row['hash']
    return panel

def track_ovrazhki(df, time_name, mun_type):
    '''
    Вспомогательная функция для build_panels, выбирающая для наблюдений
    правильные записи из справочника МО исходя из истории ОКТМО и
    совпадения типов МО.
    На входе:
    df – объект pandas.DataFrame, полученный группировкой результата мерджа наблюдений со 
    справочником МО, сгруппированный по МО и по дате.
    mun_type – string, название переменной, содержащей тип муниципалитета (может отсутствовать)
    На выходе:
    row – объект pandas.Series, 
    наиболее корректная запись в справочнике МО, подходящая данному МО и времени.    
    '''
    correct_df = df[
        df.apply(lambda x: 
                 is_proper(x, mun_type, time_name),
                 axis=1)].reset_index(drop=True).copy()
    if len(correct_df) == 0:
        correct_df = df[
            df.apply(lambda x: 
                     is_timely(x, time_name),
                     axis=1)].reset_index(drop=True).copy()        
    if len(correct_df) == 1:
        row = correct_df.iloc[0]
        row['message'] = 'ok'
        row['duplicates'] = ''
    elif len(correct_df) == 0:
        row = find_closest_row(df[time_name].unique()[0], df)
        if df[time_name].unique()[0] < pd.to_datetime('2015'):
            row['message'] = '< 2015 no guarantee'
        
        else:
            row['message'] = 'broken panel! no such municipality'
        row['duplicates'] = ''
    else:
        row = correct_df.iloc[0]
        dupls = correct_df.id.unique()
        if len(dupls) > 1:
            row['message'] = 'more than one MO with that name! see duplicates'
            row['duplicates'] = ', '.join(str(x) for x in dupls)
        else:
            row['message'] = 'ambiguous panel edge'
            row['duplicates'] = ''
    return row 

def find_closest_row(timestamp, df):
    '''
    Вспомогательная функция для track_ovrazhki, которая в случаях, если
    корректного ОКТМО в справочнике для данной даты не нашлось, ищет ближайшее, чтобы
    было куда записать сообщение об ошибке.
    На входе:
    timestamp – pd.Timestamp, дата измерения показателей.
    df – объект pandas.DataFrame, 
    полученный группировкой результата мерджа наблюдений со 
    справочником МО, сгруппированный по МО и по дате.
    На выходе:
    row – объект pandas.Series, 
    ближайшая по времени запись, не обязательно корректная. 
    '''
    min_time = df.time_start.min()
    max_time = df.time_end.max()
    if timestamp - min_time < max_time - timestamp:
        return df[df.time_start == min_time].copy().iloc[0]
    else:
        return df[df.time_end == max_time].copy().iloc[0]  
    
def is_timely(row, time_name):
    '''
    Функция, определяющая для pandas.Series,
    корректно ли по времени отнесение даты измерения
    к текущему МО с учетом его временных границ.
    '''
    return((row[time_name] >= row.time_start) & 
            (row[time_name] < row.time_end))

def is_same_mo(row, mun_type):
    return(row[mun_type] == row.mun_type_standard)

def is_proper(row, mun_type, time_name):
    mun_type_ok = True
    if mun_type:
        mun_type_ok = is_same_mo(row, mun_type)
    return mun_type_ok and is_timely(row, time_name)   
    
    
def join_panels(hash_df):
    '''
    Функция, записывающая сообщения-подсказки о возможности
    объединения панелей в одну в случаях, когда в разных интересующих 
    нас муниципалитетах был на самом деле один и тот же состав по населенным пунктам.
    На входе:
    hash_df – объект pandas.DataFrame, 
    содержащий все наиболее корректные панели,
    сгруппированный по хэшу муниципалитета.
    На выходе:
    он же, но с заполненным полем join_panels.
    '''
    hash_df['join_panels'] = ''
    if hash_df.oktmo.nunique() > 1:
        most_popular = get_mode(hash_df, 'oktmo')
        hash_df['join_panels'] = most_popular
    return hash_df

# Formatting

def truncate_to(df, to='year'):
    '''
    На входе:
    df – объект типа pandas.DataFrame с колонками time_start и time_end.
    Функция округляет периоды времени до первого дня месяца (дня года) в начале периода
    и последнего дня месяца (дня года) в конце периода.
    to – string на выбор: year, month, day.
    '''
    if to == 'year':
        df['time_start'] = pd.to_datetime(df.odate_start) + \
            pd.offsets.YearBegin(-1) 
        df['time_end'] = pd.to_datetime(df.odate_end) + \
            pd.offsets.YearBegin(0) 
    elif to == 'month':
        df['time_start'] = pd.to_datetime(df.odate_start) + \
            pd.offsets.MonthBegin(-1)
        df['time_end'] = pd.to_datetime(df.odate_end) + \
            pd.offsets.MonthBegin(0)
    else:
        df['time_start'] = pd.to_datetime(df.odate_start)
        df['time_end'] = pd.to_datetime(df.odate_end)
    return df

def split_before_2015(df, time_name):
    before_2015 = df[df[time_name] < pd.to_datetime('2015')].copy()
    before_2015['message'] = '< 2015 no data'
    after_2015 = df[df[time_name] >= pd.to_datetime('2015')].copy()
    return before_2015, after_2015

# Basic Pandas utils

def get_most_frequent_n(df, column, n):
    return df[column].value_counts()[:n].index.tolist()

def get_mode(df, column):
    return get_most_frequent_n(df, column, 1)[0]

def expand_region(df):
    df['mun_type'] = df['mun_type'].apply(
            lambda x: re.sub('[Рр]айон', 'Муниципальный район', str(x)))
    df['municipality'] = df['municipality'].apply(
            lambda x: re.sub('ский$', 'ский муниципальный район', str(x)))
    return df
