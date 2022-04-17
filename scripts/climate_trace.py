# run with
#
# Complete test data:
# python climate_trace.py -d ../test/climate-trace -c ../templates/climate-trace-specification.csv -s ../templates/ermin-specification.csv
#
# Test data with errors:
# python climate_trace.py -d ../test/climate-trace2-missing-data -c ../templates/climate-trace-specification.csv -s ../templates/ermin-specification.csv

import pandas as pd
from datetime import datetime
import re
from utils.import_data import import_data_from_local
import utils.validation as eev
import argparse
import ermin.syntax as ermin_syntax
import ermin.validation as ev

def year_to_datetime(x):
    start_time = datetime.isoformat(datetime.strptime(x.start_date, '%m/%d/%y'))
    end_time = datetime.isoformat(datetime.strptime(x.end_date, '%m/%d/%y'))
    return start_time, end_time

def create_long_df(df):
    """iterate through each value column, appending it to an empty df to make a long df"""

    long_df = pd.DataFrame(columns = ['start_time', 'end_time', 'producing_entity_id', 'emission_quantity', \
                                      'emission_quantity_units', 'emitted_product_formula', 'carbon_equivalency_method'])

    data_columns = ['CO2_emissions_tonnes', 'CH4_emissions_tonnes', 'N2O_emissions_tonnes', 'total_CO2e_20yrGWP', \
                'total_CO2e_100yrGWP']

    for data_column in data_columns:
        anchor_columns = ['start_time', 'end_time', 'producing_entity_id']
        anchor_columns.append(data_column)
        data_df = df[anchor_columns].copy() # copy to avoid warning re: setting values in a slice
        data_df.rename(columns={f'{data_column}': 'emission_quantity'}, inplace=True)
        data_df['emission_quantity_units'] = 'tonnes'
        if data_column.endswith('GWP'):
            data_df['emitted_product_formula'] = 'CO2e'
            if data_column == 'total_CO2e_100yrGWP':
                equivalency = '100-year'
            elif data_column == 'total_CO2e_20yrGWP':
                equivalency = '20-year'
            data_df['carbon_equivalency_method'] = equivalency
        else:
            data_df['emitted_product_formula'] = data_column.split('_')[0]
            data_df['carbon_equivalency_method'] = "NA"

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


        #### Step 0: Perform any manual hacking of input file to allow non-compliant inputs
        # Manually convert old-style timestamps if necessary before checking CT specification
        if 'start_date' in df and 'end_date' in df:
            if not ermin_syntax.is_valid_timestamp(df.at[0,'start_date']):
                try: 
                    df['start_date'], df['end_date'] = zip(*df.apply(year_to_datetime, axis=1))
                except ValueError:
                    errors.append(sector + ': Dates to not appear in YYYY-MM-DD or MM/DD/YY format')


        #### Step 1: check that input file matches internal CT specification and exit if not
        # USE CT specification to check input data before doing conversions
        warnings, errors = eev.check_input_dataframe(df, spec_file = args.ct_specification,
                                                     repair = False,
                                                     allow_unknown_stringtypes=True)
        if len(warnings) > 0:
            print('\nThere were ' + str(len(warnings)) + " warnings when checking sector file " + key + " against internal CT specification:")
            print('\n'.join(warnings))
        if len(errors) > 0:
            print('\nThere were ' + str(len(errors)) + " errors when checking sector file " + key + " against internal CT specification:")
            print('\n'.join(errors))
            # If Errors when checking CT spec, terminate now;  do not continue
            raise ValueError('Sector ' + key + ' did not match internal CT specification. Stopping before conversion to ERMIN format.')

        #### Step 1.5: check additional requirements specificed for CT data
        warnings, errors = eev.check_ct_requirements(df)
        if len(warnings) > 0:
            print('\nThere were ' + str(len(warnings)) + " warnings when checking sector file " + key + " against additional CT requirements:")
            print('\n'.join(warnings))
        if len(errors) > 0:
            print('\nThere were ' + str(len(errors)) + " errors when checking sector file " + key + " against additional CT requirements:")
            print('\n'.join(errors))
            # If Errors when checking additional CT reqs, terminate now;  do not continue
            raise ValueError('Sector ' + key + ' did not match additional CT requirements. Stopping before conversion to ERMIN format.')


        #### Step 2: Do conversions/additions to fit ERMIN format
        df = df.rename(columns={'start_date': 'start_time',
                           'end_date': 'end_time',
                           'iso3_country': 'producing_entity_id'})
        reshaped_df = create_long_df(df)
        reshaped_df['original_inventory_sector'] = sector
        reshaped_df['reporting_entity'] = 'climate-trace'

        print(reshaped_df)

        warnings, errors, new_df = ev.check_input_dataframe(reshaped_df, spec_file=args.ermin_specification,repair=True)
        print('\n'.join(warnings[:10]))
        print('\n'.join(errors[:10]))
        print(new_df)

        raise SystemError(0) # End after first sector for dev purposes

        # TO DO 
        #### Step 2.5: (Suggestion only) Load a key:value CSV if provided on command line,
        ####           fill in any expected missing columns intelligently
        #### Step 3: Test with ERMIN validator, get missing columns/fields
        #### Step 4: If missing columns/data, write empty key:value CSV with missing headers and exit
        #### Step 5: If nothing missing, then proceed to submit

