import pandas as pd
from datetime import datetime
from utils.import_data import import_data_from_local
import re
import numpy as np
from ermin.validation import *
from io import StringIO


# notes on any manual manipulation required before using the following script to clean edgar
# added 'edgar_' to beginning of filename
# in edgar_EDGARv6.0_FT2020_fossil_CO2_GHG_booklet2021 sheet, deleted info tab (was causing parser error)

def get_header_info(raw_df):
    df_header = raw_df.set_index('Content:')
    emitted_product_formula = df_header.loc['Compound:', 'Emissions by country and main source category']
    emissions_quantity_units = df_header.loc['Unit:', 'Emissions by country and main source category']
    measurement_method_doi_or_url = df_header.loc['Data download:',  'Emissions by country and main source category']

    return emitted_product_formula, emissions_quantity_units, measurement_method_doi_or_url


def remove_header(raw_df):
    df = raw_df.iloc[8:, :].reset_index(drop=True) # get rid of header rows for continued manipulation
    df.columns = df.iloc[0,:]
    df = df.drop(0, axis=0)

    return df


def clean_ipcc_code(x):
    for column_name in x.index:
        try:
            if re.match(r'^ipcc_code_([1-3][0-9]{3})_for_standard_report$', column_name) is not None:
                code_column_match = column_name
            elif re.match(r'^ipcc_code_([1-3][0-9]{3})_for_standard_report_name$', column_name) is not None:
                name_column_match = column_name
            else:
                continue
        except TypeError:
            continue

    return str(x[code_column_match]) + ' ' + str(x[name_column_match])


def clean_column_names(df):
    """converts year columns to integers, and drops other uneeded columns"""

    columns_to_drop = [column for column in df.columns if
                       re.match(r'ipcc_code_([1-3][0-9]{3})_for_standard_report', column)
                       is not None] + ['IPCC_annex', 'C_group_IM24_sh']
    df = df.drop(columns=columns_to_drop)
    df = df.rename(columns={'Country_code_A3': 'producing_entity_id',
                            'Name': 'producing_entity_name'})

    for column in df.columns:
        match_year = re.match(r'.*([1-3][0-9]{3})', column)
        if match_year:
            year_column = match_year.group(0)
            clean_year_column = int(year_column.lstrip('Y_'))
            df = df.rename(columns={column:clean_year_column})

    return df


def check_for_nan_columns(df):
    """a couple of csvs have nan columns that prevent following rows from running, check for them and remove them """
    isna = df.columns.isna()
    column_to_drop = df.columns[isna]
    if isna.sum() >= 1:
        return df.drop(columns=column_to_drop)
    else:
        return df


def year_int_to_datetime(x):
    return datetime.isoformat(datetime.strptime(str(x.year), '%Y'))


if __name__ == "__main__":
    edgar_dictionary = import_data_from_local('edgar')
    keys_to_pop = ['TOTALS BY COUNTRY']

    for key in edgar_dictionary.keys():
        if re.match(r".+1996", key) is not None:
            keys_to_pop.append(key)

    # removing the sheets 'TOTALS BY COUNTRY' and the sheets with 1996 IPCC codes
    [edgar_dictionary.pop(key) for key in keys_to_pop]
    edgar_dictionary_clean = {}

    for key, df_with_header in edgar_dictionary.items():
        print(key)
        emitted_product_formula, emissions_quantity_units, measurement_method_doi_or_url = \
            get_header_info(df_with_header)
        df = remove_header(df_with_header)
        df['original_inventory_sector']  = df.apply(clean_ipcc_code, axis=1)
        df = check_for_nan_columns(df)
        df = clean_column_names(df)
        year_columns= [col for col in df.columns if re.match(r'\d{4}', str(col)) is not None]
        # summing bio and fossil totals for each country/sector
        df = df.groupby(by = ['producing_entity_name', 'producing_entity_id','original_inventory_sector'], as_index=False)[year_columns].sum()
        df = df.melt(id_vars = ['producing_entity_id', 'producing_entity_name', 'original_inventory_sector'],
                     var_name = 'year',
                     value_name = 'emissions_quantity')
        df['start_time'] = df.apply(year_int_to_datetime, axis=1)
        df = df.drop(columns=['year'])
        df['emitted_product_formula'] = emitted_product_formula
        df['emissions_quantity_units'] = emissions_quantity_units
        df['measurement_method_doi_or_url'] = measurement_method_doi_or_url
        df['reporting_entity'] = 'edgar'


# clean  data
# add extra information
# check against ermin standards
# upload to database

