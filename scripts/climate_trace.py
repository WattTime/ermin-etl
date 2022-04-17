# run with
#
# Using reduced test data:
# python climate_trace.py -d ../test/climate-trace3-reduced_input -c ../templates/climate-trace-specification.csv -s ../templates/ermin-specification.csv -v -o errors.txt
#
# Using reduced test data, write missing values to CSV
# python climate_trace.py -d ../test/climate-trace3-reduced_input -c ../templates/climate-trace-specification.csv -s ../templates/ermin-specification.csv -v -o errors.txt -M missing_values.csv
#
# Using reduced test data, fill missing values from CSV
# time python climate_trace.py -d ../test/climate-trace3-reduced_input -c ../templates/climate-trace-specification.csv -s ../templates/ermin-specification.csv -v -o errors.txt -m ../test/climate-trace3-reduced_input/fill_values_table.csv
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
from collections import defaultdict
from pathlib import Path

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
    parser.add_argument('-o','--error_output', metavar='filename', type=str, default=None,
                        help='Path to output text file to contain all warnings and errors.')
    parser.add_argument('-m','--missing_value_input', metavar='filename', type=str, default=None,
                        help='Missing value input file (expects sector, field, value CSV to fill missing values).')
    parser.add_argument('-M','--missing_value_output', metavar='filename', type=str, default=None,
                        help='Missing value output file (will write sector, field, NULL CSV for each missing field).')
    parser.add_argument('-v', '--verbose', help='More verbose output',
                        action='store_true')
    args = parser.parse_args()

    climate_trace_dictionary = import_data_from_local(
            reporting_entity='climate-trace',
            path_to_data=args.datadir,
            verbose=args.verbose)

    ct_warnings = defaultdict(list) # from CT specification checking, keyed by sector
    ct_errors = defaultdict(list) # from CT specification checking, keyed by sector
    ermin_warnings = defaultdict(list) # from ERMIN specification checking, keyed by sector
    ermin_errors = defaultdict(list) # from ERMIN specification checking, keyed by sector
    missing_values = {} # dict of missing fields keyed by sector
    fill_values = defaultdict(list) # dict of lists of [column, value], keyed by sector

    # Load missing values fill table, if given
    if args.missing_value_input is not None:
        with open(args.missing_value_input, 'r') as f:
            for line in f:
                words = line.split(',')
                # format of fill_values is {sector:[(column, value), (column, value),...]}
                fill_values[words[0]].append((words[1],words[2]))

    # Loop through sectors, validating each table
    for key, df in climate_trace_dictionary.items():
        sector = key.split('_')[0]
        date = key.split('_')[1] # do something with the date later to get version
        if args.verbose:
            print("Sector: " + sector)
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
            print('\nThere were ' + str(len(warnings)) + " warnings when checking sector file " + sector + " against internal CT specification:")
            print('\n'.join(warnings))
            ct_warnings[sector] += warnings
        if len(errors) > 0:
            print('\nThere were ' + str(len(errors)) + " errors when checking sector file " + sector + " against internal CT specification:")
            print('\n'.join(errors))
            # If Errors when checking CT spec, terminate now;  do not continue
            errors.append('Sector ' + sector + ' did not match internal CT specification. Skipping sector before checking additional CT requirements.')
            ct_errors[sector] += errors
            continue # Continue to next sector.

        #### Step 1.5: check additional requirements specificed for CT data
        warnings, errors = eev.check_ct_requirements(df, sector=sector)
        ct_warnings[sector] += warnings
        if len(errors) > 0:
            errors.append('Sector ' + sector + ' did not match additional CT requirements. Skipping sector before conversion to ERMIN format.')
            ct_errors[sector] += errors
            continue # Continue to next sector.

        #### Step 2: Do conversions/additions to fit ERMIN format
        df = df.rename(columns={'start_date': 'start_time',
                           'end_date': 'end_time',
                           'iso3_country': 'producing_entity_id'})
        reshaped_df = create_long_df(df)
        reshaped_df['original_inventory_sector'] = sector
        reshaped_df['reporting_entity'] = 'climate-trace'
        reshaped_df['producing_entity_name'] = ''
        # Replace producing_entity with country name from COUNTRIES_DICT
        for i,row in reshaped_df.iterrows():
            country_code = row['producing_entity_id']
            country_name = eev.COUNTRIES_DICT[country_code]
            row['producing_entity_name'] = country_name

        # TO DO 
        #### Step 2.5: Load a key:value CSV if provided on command line,
        ####           fill in any expected missing columns intelligently
        if sector in fill_values:
            for keyvalue_tuple in fill_values[sector]:
                column = keyvalue_tuple[0]
                value = keyvalue_tuple[1]
                print('Sector ' + sector + ', filling column ' + column + ' with value ' + value)
                reshaped_df[column] = value


        #### Step 3: Test with ERMIN validator, get missing columns/fields
        warnings, errors = ev.check_input_dataframe(reshaped_df, spec_file=args.ermin_specification,repair=False)

        ermin_warnings[sector] += warnings
        if len(errors) > 0:
            errors.append('Sector ' + sector + ' did not match additional ERMIN specification. Stopping before DB upload.')
            ermin_errors[sector] += errors
            continue # skip to next sector; do not continue to process this sector

        #### TO DO: Step 4: If nothing missing, then proceed to submit to DB



    #### All sectors processed, report errors (and save to file)
    for key in ct_errors:
        print('\nSector ' + key + ' encountered errors when checking CT requirements, ERMIN conversion skipped (printing up to 10):')
        print('\n'.join(ct_errors[key][:10]))

    for key in ermin_errors:
        print('\nSector ' + key + ' encountered errors when checking ERMIN requirements, DB upload skipped (printing up to 10):')
        print('\n'.join(ermin_errors[key][:10]))

    if args.error_output is not None:
        print('Writing all warnings and errors to output file ' + args.error_output)
        # write new output file containing all warnings and errors
        path = Path(args.error_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(args.error_output,'w') as f:
            for key in climate_trace_dictionary.keys():
                sector = key.split('_')[0]
                if len(ct_warnings[sector]) > 0:
                    f.write('\nSector ' + sector + ' encountered warnings when checking CT requirements:')
                    f.write('\n'.join(ct_warnings[sector]))
                if len(ct_errors[sector]) > 0:
                    f.write('\nSector ' + sector + ' encountered errors when checking CT requirements:')
                    f.write('\n'.join(ct_warnings[sector]))
                if len(ermin_warnings[sector]) > 0:
                    f.write('\nSector ' + sector + ' encountered warnings when checking ERMIN requirements:')
                    f.write('\n'.join(ermin_warnings[sector]))
                if len(ermin_errors[sector]) > 0:
                    f.write('\nSector ' + sector + ' encountered errors when checking ERMIN requirements:')
                    f.write('\n'.join(ermin_warnings[sector]))


    #### Step 5: If missing columns/data, write empty key:value CSV with missing headers and exit
    # Write missing value output file if requested
    if args.missing_value_output is not None:
        print('Writing missing columns CSV to output file ' + args.missing_value_output)
        # write new output file containing missing values in CSV forma
        path = Path(args.missing_value_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(args.missing_value_output,'w') as f:
            for key in climate_trace_dictionary.keys():
                sector = key.split('_')[0]
                if sector in ermin_errors:
                    errors = ermin_errors[sector]
                    # find all missing columns, e.g. errors in this format
                    # Missing this required column: "unfccc_annex_1_category".
                    for error in errors:
                        if error.startswith('Missing this required column: "'):
                            column = error[31:-2] # remove the start and end of the error
                            f.write(','.join([sector, column,'NULL']) + '\n')


