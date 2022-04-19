from datetime import datetime
import pandas as pd


def record_version(reporting_entity, sector, date, path_to_version_csv):
    """date comes in as YYYYMMDD"""

    version_df = pd.read_csv(path_to_version_csv)

    specific_version_info = version_df[(version_df.reporting_entity == reporting_entity) & \
                                       (version_df.sector == sector)].copy()
    date = datetime.strptime(date, '%Y%m%d')

    if specific_version_info.empty:
        version = '0.0'
        changelog = 'initial commit, first round of data'
    else:
        specific_version_info['date'] = pd.to_datetime(specific_version_info['date'])
        version_index = version_df.columns.get_loc('version')
        date_index = version_df.columns.get_loc('date')
        date_of_last_version = specific_version_info.sort_values(by='date', ascending=False).iloc[0,date_index]
        version = specific_version_info.sort_values(by='date', ascending=False).iloc[0, version_index]

        if date <= date_of_last_version:
            return
        else:
            version += 0.1
            changelog = 'NULL'

    row = pd.DataFrame({'reporting_entity':[reporting_entity],
                         'sector': [sector],
                         'date': [date],
                         'version': [version],
                        'changelog': [changelog]})

    version_df = pd.concat([version_df, row], axis=0)
    version_df.to_csv('versioning.csv', index=False)


