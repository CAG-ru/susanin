import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pathlib
import logging
from copy import copy
import numpy as np


class Geomatch:
    def __init__(self,
                 standard_db=False,
                 # порядок колонок имеет значение т.к. определяет порядок фильтрации множеств
                 match_columns=['region', 'settlement', 'municipality'],
                 threshold=None,
                 debug=False,
                 skip_dead_end_fields=0,
                 rearrange_score=0.9,
                 filter_by_prev=True
                 ):
        self.current_directory = str(pathlib.Path(__file__).parent.resolve())

        if type(standard_db) is pd.core.frame.DataFrame:
            self.standard = standard_db.reset_index(drop=True)
        elif type(standard_db) is str:
            self.standard = pd.read_csv(pathlib.Path(f'{standard_db}'), compression='zip')
        else:
            self.standard = pd.read_csv(
                pathlib.Path(f'{self.current_directory}/standard.zip'),
                compression='zip',
                delimiter=';'
            )
        self.standard = self.standard.fillna('')
        self.rearrange_score = rearrange_score

        # здесь можно задать threshold срабатывания для полей если не задано то threshold = 0
        if threshold is not None:
            self.threshold = threshold
        else:
            self.threshold = {}

        if debug:
            logging.basicConfig(filename='debug.log', format='%(asctime)s - %(levelname)s \n%(message)s', level=logging.DEBUG)

        self.skip_dead_end_fields = skip_dead_end_fields
        self.vectorizers = {}
        self.vectors = {}
        self.ids = {}
        self.keys = {}
        self.field_ids = {}
        self.filter_ids = None
        self.match_columns = match_columns
        self.base_match_columns = match_columns
        # self.standard = self.standard  # [self.match_columns]
        self.filter_by_prev = filter_by_prev


        # todo: протестировать другие варианты
        # инициализируем tfidf по каждой колонке
        for field in self.match_columns:
            self.ids[field] = self.standard.reset_index().groupby(field)['index'].apply(list).to_dict()
            self.keys[field] = list(self.ids[field].keys())
            self.field_ids[field] = list(self.ids[field].values())
            self.field_ids[field] = {value: k for k, v in enumerate(self.field_ids[field]) for value in self.field_ids[field][k]}

            self.vectorizers[field] = TfidfVectorizer(ngram_range=(1, 4), analyzer='char_wb')
            self.vectors[field] = self.vectorizers[field].fit_transform(self.keys[field])

            # print(self.standard.groupby(field)['index'].apply(list).to_dict())
            # print(len(self.ids[field]))

    def get_output(self, scores, field, top_n=1):
        out = []

        if scores is None:
            out = [[{'name': '', 'score': 0, 'ids': set()}]]
        else:
            for score in scores:
                row = []
                for i in (-score).argsort()[:top_n]:
                    id = i
                    if self.filter_ids:
                        id = self.get_filter_ids(field)[i]

                    curr_key = self.keys[field][id]
                    ids = self.ids[field][curr_key]
                    row.append({
                        'id': id,
                        'name': curr_key,
                        'score': score[i],
                        'ids': set(ids)
                    })
                out.append(row)

        return out

    def get_filter_ids(self, field):
        return np.array(list({self.field_ids[field][i] for i in self.filter_ids}))

    def find_similar_records(self, data, field):
        vec = self.vectorizers[field].transform(data).toarray()
        vectors = self.vectors[field]
        if self.filter_ids:
            vectors = vectors[self.get_filter_ids(field), :]

        out = cosine_similarity(
            vec,
            vectors
        )

        return out

    def return_matched_rows(self, ids, max_rows=1):
        if ids:
            selected_record = list(ids)[0]
            return dict(self.standard.iloc[selected_record])
        else:
            return {i: '' for i in self.match_columns}

    def find_in_field(self, address_field, field, top_n=5):
        scores = None
        if address_field != '':
            scores = self.find_similar_records([address_field], field)

        return self.get_output(scores, field, top_n)



    def find_in_fields_by_step(self, address_dict):
        if type(address_dict) is list:
            address_dict = self.__to_list_of_dict__(address_dict)

        out = {}
        self.filter_ids = None
        for field in self.match_columns:
            if field in address_dict:
                out[field] = self.find_in_field(address_dict[field], field)
                if self.filter_by_prev:
                    self.filter_ids = out[field][0][0]['ids']

        return self.__to_dict_of_list__(out)

    def find_in_fields_by_intersection(self, address_dict):
        if type(address_dict) is list:
            address_dict = self.__to_list_of_dict__(address_dict)

        out = {}
        for field in self.match_columns:
            if field in address_dict:
                out[field] = self.find_in_field(address_dict[field], field)

        return self.__to_dict_of_list__(out)

    def check_threshold(self, field, score):
        if field in self.threshold:
            threshold = self.threshold[field]
        else:
            threshold = 0
        return score > threshold

    def filter_by_field_intersection_old(self, search_results):
        results = []
        for search_result in search_results:
            ids = []
            skip_dead_end_fields = 0
            curr_query = {
                'ids': []
            }
            for field in search_result:
                if self.check_threshold(field, search_result[field][0]['score']):
                    test_ids = copy(ids)
                    test_ids.append(search_result[field][0]['ids'])
                    search_result_field = search_result[field][0]

                    # здесь происходит проверка на тупиковые множества
                    if len(set.intersection(*test_ids)) > 0 or self.skip_dead_end_fields <= skip_dead_end_fields:
                        ids = test_ids
                    else:
                        skip_dead_end_fields += 1
                        logging.debug(f'action: SKIP_DEAD_END_FIELD \n'
                                     f'field: {field} \n'
                                     f'name: {search_result_field["name"]} \n'
                                     f'score: {search_result_field["score"]}\n'
                                     f'ids: {search_result_field["ids"]}\n')

                    logging.debug(f'action: INFO \n'
                                 f'field: {field} \n'
                                 f'name: {search_result_field["name"]} \n'
                                 f'score: {search_result_field["score"]}\n'
                                 f'ids: {search_result_field["ids"]}\n')

                # curr_query[f'{field}_name'] = search_result_field['name']
                # curr_query[f'{field}_score'] = search_result_field['score']

            if len(ids) > 0:
                curr_query['ids'] = set.intersection(*ids)
            curr_query['results'] = len(curr_query['ids'])
            curr_query['skiped_dead_end_fields'] = skip_dead_end_fields

            results.append(curr_query)

        return results

    def filter_by_fields_intersections(self, search_result, fields):
        ids = []
        total_score = 0
        used_fields = 0
        skip_dead_end_fields = 0
        out_fields = {}
        curr_query = {
            'ids': []
        }

        for field in fields:

            search_result_field = search_result[field][0]

            out_fields[field] = {
                'val': search_result_field['name'],
                'score': search_result_field['score'],
                'status': 'LOW_SCORE'
            }

            if self.check_threshold(field, search_result_field['score']):
                test_ids = copy(ids)
                test_ids.append(search_result_field['ids'])

                if len(set.intersection(*test_ids)) > 0 or self.skip_dead_end_fields <= skip_dead_end_fields:
                    total_score += search_result_field['score']
                    used_fields += 1
                    out_fields[field]['status'] = 'OK'
                    ids = test_ids
                else:
                    out_fields[field]['status'] = 'DEAD_END_SKIP'
                    skip_dead_end_fields += 1

        if len(ids) > 0:
            curr_query['ids'] = set.intersection(*ids)

        curr_query['results'] = len(curr_query['ids'])
        curr_query['skiped_dead_end_fields'] = skip_dead_end_fields
        curr_query['out_fields'] = out_fields
        curr_query['score'] = total_score / used_fields if used_fields > 0 else 0

        return curr_query

    def filter_by_field_intersection(self, search_results):
        for search_result in search_results:
            result = self.filter_by_fields_intersections(search_result, search_result.keys())

        return result

    def output_data(self, intersection):
        result = self.return_matched_rows(intersection['ids'])
        if 'results' in intersection:
            result['results_count'] = intersection['results']
        if 'skiped_dead_end_fields' in intersection:
            result['skiped_dead_end_fields'] = intersection['skiped_dead_end_fields']
        if 'out_fields' in intersection:
            result['out_fields'] = intersection['out_fields']
        if 'score' in intersection:
            result['score'] = intersection['score']

        return result

    def __extract_result(self, row):
        return self.process_address(row.to_dict())

    def process_df(self, df):
        return df.apply(self.__extract_result, axis=1, result_type="expand")

    def find_best_result(self, address_dict):
        matches = self.find_in_fields_by_step(address_dict)
        intersection = self.filter_by_field_intersection(matches)

        return self.output_data(intersection)

    def process_address(self, address_dict):
        curr_score = 0
        for i in range(len(self.match_columns)):
            f = self.match_columns.pop(0)
            self.match_columns.append(f)
            self.filter_ids = False
            curr_result = self.find_best_result(address_dict)
            if curr_result['score'] > curr_score:
                curr_score = curr_result['score']
                result = curr_result
            if curr_score > self.rearrange_score:
                break

        self.match_columns = self.base_match_columns
        return result

    def __to_dict_of_list__(self, dict_of_list):
        return [dict(zip(dict_of_list, t)) for t in zip(*dict_of_list.values())]

    def __to_list_of_dict__(self, list_of_dict):
        return {k: [dic[k] for dic in list_of_dict] for k in list_of_dict[0]}

    def __call__(self, input_data):
        if isinstance(input_data, pd.DataFrame):
            return self.process_df(input_data)
        elif isinstance(input_data, pd.Series):
            return self.process_address(input_data.to_dict())
        else:
            return self.process_address(input_data)
