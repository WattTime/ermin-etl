import pandas as pd
import numpy as np

data_requirements = pd.read_csv('climate-trace_subsector_data_requiremnets.csv')
required_dates_countries = pd.read_csv('climate-trace_required_dates_countries.csv')


for subsector in data_requirements.subsector:
    columns = data_requirements.drop(['sector', 'subsector'], axis=1).columns
    df = pd.DataFrame(columns=columns)
    data_columns_bool = data_requirements[data_requirements.subsector == subsector].drop(['sector', 'subsector', 'begin_date', 'end_date'], axis=1)
    df[['begin_date', 'end_date', 'iso3_country']] = required_dates_countries[['begin_date', 'end_date', 'iso3_country']]
    value_columns = columns.drop(['begin_date', 'end_date', 'iso3_country'])

    for column in value_columns:
        if data_columns_bool[f'{column}'].values[0] == True:
            if subsector in ['other-agricultural-soil-emissions', 'forest-sink', 'net-forest-emissions']:
                df[column] = -100000
            else:
                df[column] = 1000000

    df.to_csv(f'climate-trace_{subsector}-test_20220403.csv')