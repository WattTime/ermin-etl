# run with
# python climate_trace.py -d ../test2 -c ../templates/climate-trace-specification.csv
import pandas as pd
from datetime import datetime
import re
from utils.import_data import import_data_from_local
import utils.validation as eev
import argparse
import ermin.syntax as ermin_syntax


def year_to_datetime(x):
    start_time = datetime.isoformat(datetime.strptime(x.start_date, '%m/%d/%y'))
    end_time = datetime.isoformat(datetime.strptime(x.end_date, '%m/%d/%y'))
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
        data_df = df[anchor_columns].copy() # copy to avoid warning re: setting values in a slice
        data_df.rename(columns={f'{data_column}': 'emissions_quantity'}, inplace=True)
        data_df['emissions_quantity_units'] = 'tonnes'
        if data_column.endswith('GWP'):
            data_df['emitted_product_formula'] = 'CO2e'
            # TO DO: check equivalency conversion, not working on "other-onsite-fuel-usage-test_20220403" file
            equivalency = re.findall(r'\d{2,3}', data_column.strip('GWP')[-4:])[0] + '-year'
            data_df['carbon_equivalency_method'] = equivalency
        else:
            data_df['emitted_product_formula'] = data_column.split('_')[0]
        long_df = pd.concat([long_df, data_df]) # use concat as append was deprecated
    return long_df


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--ct_specification', metavar='filename', type=str, 
                        help='Path to CSV file giving CT specification.')
    parser.add_argument('-s','--ermin_specification', metavar='filename', type=str, 
                        help='Path to CSV file giving ERMIN specification.')
    parser.add_argument('-d','--datadir', metavar='filename', type=str, default=None,
                        help='Path to directory containing input data (default None).')
    parser.add_argument('-a','--all_errors', action='store_true',
                        help='Print all warnings and errors to stdout (default print up to 10).')
    parser.add_argument('-v', '--verbose', help='More verbose output',
                        action='store_true')
    args = parser.parse_args()

    climate_trace_dictionary = import_data_from_local(
            reporting_entity='climate-trace',
            path_to_data=args.datadir,
            verbose=args.verbose)

    warnings = []
    errors = []

    # Loop through sectors, validating each table
    for key, df in climate_trace_dictionary.items():
        sector = key.split('_')[0]
        date = key.split('_')[1] # do something with the date later to get version

        try:
            df = df.drop(columns=['Unnamed: 0'])
        except KeyError:
            pass


        #### Step 1: check that input file matches internal CT specification
        # Manually old-style timestamps if necessary before checking CT specification
        if 'start_date' in df and 'end_date' in df:
            if not ermin_syntax.is_valid_timestamp(df.at[0,'start_date']):
                try: 
                    df['start_date'], df['end_date'] = zip(*df.apply(year_to_datetime, axis=1))
                except ValueError:
                    errors.append(sector + ': Dates to not appear in YYYY-MM-DD or MM/DD/YY format')

        # USE CT specification to check input data before doing conversions
        warnings, errors = eev.check_input_dataframe(df, spec_file = args.ct_specification,
                                                     allow_unknown_stringtypes=True)
        if len(warnings) > 0:
            print('\nThere were ' + str(len(warnings)) + " warnings when checking sector file " + key + " against internal CT specification:")
            print(warnings)
        if len(errors) > 0:
            print('\nThere were ' + str(len(errors)) + " errors when checking sector file " + key + " against internal CT specification:")
            print(errors)
            # If Errors when checking CT spec, terminate now;  do not continue
            raise ValueError('Sector ' + key + ' did not match internal CT specification. Stopping before conversion to ERMIN format.')


        #### Step 2: Now do conversions/additions to fit ERMIN format
        df = df.rename(columns={'start_date': 'start_time',
                           'end_date': 'end_time',
                           'iso3_country': 'producing_entity_id'})
        reshaped_df = create_long_df(df)

        reshaped_df['original_inventory_sector'] = sector
        reshaped_df['reporting_entity'] = 'climate-trace'
        
        # TO DO 
        # test with ermin_validator
        # write empty csv with missing headers




