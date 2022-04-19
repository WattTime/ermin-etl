from climate_trace import main
from utils.database import *
from datetime import datetime


current_timestamp = datetime.now()

record_missing_input = False
fill_missing_columns = True
push_to_db = True

kwargs = {
          'ct_specification': '../templates/climate-trace-specification.csv',
          'ermin_specification':'../templates/ermin-specification.csv',
          'datadir': '../test/climate-trace',
          'all_errors': True,
          'error_output': 'errors_ct.txt',
          'missing_value_input': '/Users/christyjlewis/ermin-etl/missing_values/filled_values_climate-trace.csv',
          'missing_value_output': '/Users/christyjlewis/ermin-etl/missing_values/missing_values_climate-trace.csv',
          'verbose': True
          }
# missing_value_path = '../supplemental_information'
# ct_specification = '../templates/climate-trace-specification.csv'
# ermin_specification = '../templates/ermin-specification.csv'
# datadir = '../test/climate-trace'
# all_errors = True
# error_output = 'errors_ct.txt'
# missing_value_input = '../missing_values/filled_values_climate-trace.csv'
# missing_value_output = '../missing_values/missing_values_climate-trace.csv'
# verbose = True


if __name__ == '__main__':

    if record_missing_input:
        kwargs['missing_value_input'] = None
        main(**kwargs)

    if fill_missing_columns:
        kwargs['missing_value_output'] = None
        filled_values = pd.read_csv(kwargs['missing_value_input'],
                                  names=['sector', 'missing_column', 'input'])
        idx = filled_values[filled_values.missing_column == 'reporting_timestamp'].index
        filled_values.loc[idx, 'input'] = datetime.isoformat(current_timestamp)
        versions = pd.read_csv('versioning.csv')

        for sector in filled_values.sector.unique():
            print(sector)
            version = versions[(versions.sector == sector)]['version'].values
            changelog = versions[(versions.sector == sector)]['changelog'].values[0]
            changelog_idx = filled_values[(filled_values.sector == sector) & (filled_values.missing_column == 'data_version_changelog')].index
            version_idx = filled_values[(filled_values.sector == sector) & (filled_values.missing_column == 'data_version')].index
            filled_values.loc[version_idx, 'input'] = version
            filled_values.loc[changelog_idx, 'input'] = changelog

        filled_values.to_csv(kwargs['missing_value_input'],header = False, index=False) # get rid of index when writing

        reshaped_clean_data, errors, warnings = main(**kwargs)

    if push_to_db:
        if len(errors) > 0:
            print('Errors need to be resolved. Check errors report.')
        else:
            for key, value in reshaped_clean_data.items():
                value.to_csv(f'{key}.csv')
                insert_clean_data(value)


