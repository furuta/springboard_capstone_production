import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import luigi
import pickle
import datetime
import time
import locale
import requests
import json
import statsmodels.api as sm
import os


class ModifyCalendarDataTask(luigi.Task):
    calendar_csv_filename = luigi.Parameter()

    def output(self):

    def run(self):
        df_calendar = pd.read_csv(self.calendar_csv_filename)

        #########################
        # pregare calendar data #
        #########################
        use_columns_in_calendar = [
            'listing_id',
            'date',
            # 'available',
            'price',
        ]
        df_calendar = df_calendar.loc[0:1000, use_columns_in_calendar]

        # price
        df_calendar['price_amount'] = df_calendar['price'].map(
            lambda x: float(str(x).replace(',', '').replace('$', '')))
        df_calendar.loc[df_calendar['price_amount'] > 0, :]
        # date
        locale.setlocale(locale.LC_TIME, 'ja_JP')
        df_calendar['datetime'] = df_calendar['date'].map(
            lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d'))
        # df_calendar['year'] = df_calendar['datetime'].map(lambda x:x.year)
        df_calendar['month'] = df_calendar['datetime'].map(lambda x: x.month)
        df_calendar['day'] = df_calendar['datetime'].map(lambda x: x.day)
        df_calendar['day_of_week'] = df_calendar['datetime'].map(
            lambda x: x.weekday())
        df_calendar = pd.get_dummies(
            df_calendar, columns=['month', 'day_of_week'])
        del df_calendar['date']
        del df_calendar['price']
        del df_calendar['datetime']


class ModifyListingDataTask(luigi.Task):
    listings_csv_filename = luigi.Parameter()

    def output(self):

    def run(self):
        df_listing = pd.read_csv(self.listings_csv_filename)

        ########################
        # pregare listing data #
        ########################
        use_columns_in_listing = [
            'id',
            'latitude',
            'longitude',
            'property_type',
            'room_type',
            'accommodates',
            'bedrooms',
            'beds',
            'cancellation_policy',
        ]
        df_listing = df_listing.loc[:, use_columns_in_listing]
        df_listing = df_listing.rename(columns={'id': 'listing_id'})
        df_listing = self.__getGooglePlaceData(df_listing)

        del df_listing['latitude']
        del df_listing['longitude']

        # property_type, room_type, cancellation_policy
        df_listing = pd.get_dummies(
            df_listing, columns=['property_type', 'room_type', 'cancellation_policy'])


class MargeNeighborhoodDataTask(luigi.Task):
    neighborhood_data_file = luigi.Parameter()
    intermediate_data_with_neighborhood_file = luigi.Parameter()
    google_places_api_url = luigi.Parameter()
    api_key = luigi.Parameter()
    radius = '300'
    language = 'en'

    def requires(self):

    def output(self):

    def run(self):
        # TODO:This should be managed with DB
        neighborhood_data_filepath = self.intermediate_data_path + \
            self.neighborhood_data_file + self.radius + '.pkl'
        if os.path.exists(neighborhood_data_filepath):
            df_neighborhood = pd.read_pickle(neighborhood_data_filepath)
        else:
            df_neighborhood = pd.DataFrame(
                [], columns=['latitude', 'longitude', 'results', 'created'])

        for index, row in df_listing.iterrows():
            # Because the difference is less than 10m, round off to the four decimal places
            latitude_round = round(row.latitude, 4)
            longitude_round = round(row.longitude, 4)

            # find of neighborhood data
            neighborhood = df_neighborhood[(df_neighborhood['latitude'] == latitude_round) & (
                df_neighborhood['longitude'] == longitude_round)]

            # get only when there is no data
            if neighborhood.empty:
                # if not exist, get data from api
                response = requests.get(self.google_places_api_url +
                                        'key=' + self.api_key +
                                        '&location=' + str(latitude_round) + ',' + str(longitude_round) +
                                        '&radius=' + self.radius +
                                        '&language=' + self.language)
                data = response.json()

                neighborhood = pd.DataFrame(
                    [latitude_round, longitude_round, data['results'], time.time()], index=df_neighborhood.columns).T
                df_neighborhood = df_neighborhood.append(neighborhood)

                with open(neighborhood_data_filepath, "wb") as target:
                    pickle.dump(df_neighborhood, target)

            for result in neighborhood.at[0, 'results']:
                column_name = 'neighborhood_' + result['types'][0]
                if not column_name in df_listing.columns:
                    df_listing[column_name] = 0
                df_listing.loc[index, column_name] += 1

        return df_listing


class MargeAndPrepareDataTask(luigi.Task):
    intermediate_data_path = luigi.Parameter()
    intermediate_data_file = luigi.Parameter()

    def requires(self):

    def output(self):
        return luigi.LocalTarget(self.intermediate_data_path + self.intermediate_data_file + '.pkl')

    def run(self):
        ####################
        # marge and output #
        ####################
        df_intermediate = pd.merge(df_listing, df_calendar, on='listing_id')
        del df_intermediate['listing_id']

        with open(self.output().path, "wb") as target:
            pickle.dump(df_intermediate, target)


class CreateModelTask(luigi.Task):
    def requires(self):
        return DataPreparingTask()

    def run(self):
        with open(self.input().path, 'rb') as f:
            df_intermediate = pickle.load(f)

        y = df_intermediate['price_amount']
        X = df_intermediate.drop('price_amount', axis=1)

        #
        mod = sm.OLS(y, sm.add_constant(X))
        result = mod.fit()
        # predict = result.predict(X)

        print(result.summary())

    def output(self):
        return


class TestModelTask(luigi.Task):
    def requires(self):

    def output(self):

    def run(self):


class PredictModelTask(luigi.Task):
    def requires(self):

    def output(self):

    def run(self):


class DataPreparingTask(luigi.Task):
    def requires(self):

    def output(self):

    def run(self):


if __name__ == '__main__':
    # luigi.run(['DataPreparingTask', '--workers', '1', '--local-scheduler'])
    luigi.run(['CreateModelTask', '--workers', '1', '--local-scheduler'])
