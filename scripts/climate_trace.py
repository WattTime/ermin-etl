import pandas as pd
from datetime import datetime
import re
from utils.import_data import import_data_from_local


def year_to_datetime(x):
    start_time = datetime.isoformat(datetime.strptime(x.start_time, '%m/%d/%y'))
    end_time = datetime.isoformat(datetime.strptime(x.end_time, '%m/%d/%y'))
    return start_time, end_time


def create_long_df(df):
    """iterate through each value column, appending it to an empty df to make a long df"""

    long_df = pd.DataFrame(columns = ['start_time', 'end_time', 'producing_entity_id', 'emissions_quantity', \
                                      'emissions_quantity_units', 'emitted_product_formula', 'carbon_equivalency_method'])

    data_columns = ['CO2_emissions_tonnes', 'CH4_emissions_tonnes', 'N2O_emissions_tonnes', 'total_CO2e_20yrGWP', \
                'total_CO2e_100yrGWP']

    for data_column in data_columns:
        anchor_columns = ['start_time', 'end_time', 'producing_entity_id']
        anchor_columns.append(data_column)
        data_df = df[anchor_columns]
        data_df.rename(columns={f'{data_column}': 'emissions_quantity'}, inplace=True)
        data_df['emissions_quantity_units'] = 'tonnes'
        if data_column.endswith('GWP'):
            data_df['emitted_product_formula'] = 'CO2e'
            equivalency = re.findall(r'\d{2,3}', data_column.strip('GWP')[-4:])[0] + '-year'
            data_df['carbon_equivalency_method'] = equivalency
        else:
            data_df['emitted_product_formula'] = data_column.split('_')[0]
        long_df = long_df.append(data_df)

    return long_df


if __name__ == '__main__':

    climate_trace_dictionary = import_data_from_local('climate-trace')

    for key, df in climate_trace_dictionary.items():
        sector = key.split('_')[0]
        date = key.split('_')[1] # do something with the date later to get version

        try:
            df = df.drop(columns=['Unnamed: 0'])
        except KeyError:
            pass

        df = df.rename(columns={'begin_date': 'start_time',
                           'end_date': 'end_time',
                           'iso3_country': 'producing_entity_id'})

        df['start_time'], df['end_time'] = zip(*df.apply(year_to_datetime, axis=1))
        reshaped_df = create_long_df(df)
        reshaped_df['original_inventory_sector'] = sector
        reshaped_df['reporting_entity'] = 'climate-trace'
        # test with ermin_validator
         # write empty csv with missing headers




