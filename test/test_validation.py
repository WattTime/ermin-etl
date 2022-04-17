import pandas as pd
import utils.validation as eev
from datetime import datetime

def test_check_ct_requirements():
    """Ensure checking internal CT requirements works properly
    """

    # Read a pandas dataframe from a file
    df = pd.read_csv('climate-trace2-missing-data/climate-trace_aluminum-test_20220403.csv', comment='#', keep_default_na=False)

    # Convert date strings to ISO format
    df['start_date'] = [datetime.isoformat(datetime.strptime(datestr, '%m/%d/%y')) for datestr in df.start_date]
    df['end_date'] = [datetime.isoformat(datetime.strptime(datestr, '%m/%d/%y')) for datestr in df.end_date]

    # Alumninum sector on these test data should 
    # return extra error due to negative value
    warnings, errors = eev.check_ct_requirements(df, sector = 'aluminum')

    expected_warnings = []
    expected_errors = ['Error: Negative total_CO2e_100yrGWP emissions -1000000.0 reported in 2017 for country ARM','Error: Data for country ABW starts on 2016-01-01, requirement is on or before 2015-01-01', 'Error: Entry spans more than one year: 2017-01-01T00:00:00\t2018-12-31T00:00:00\tAFG', 'Error: country AGO missing from input table.']

    for warning in warnings:
        assert warning in expected_warnings 
    assert len(warnings) == len(expected_warnings)
    
    for error in errors:
        assert error in expected_errors
    assert len(errors) == len(expected_errors)



    # No error re: negative value for net-forest-emissions sector
    warnings, errors = eev.check_ct_requirements(df, sector = 'net-forest-emissions')

    expected_warnings = []
    expected_errors = ['Error: Data for country ABW starts on 2016-01-01, requirement is on or before 2015-01-01', 'Error: Entry spans more than one year: 2017-01-01T00:00:00\t2018-12-31T00:00:00\tAFG', 'Error: country AGO missing from input table.']

    for warning in warnings:
        assert warning in expected_warnings 
    assert len(warnings) == len(expected_warnings)
    
    for error in errors:
        assert error in expected_errors
    assert len(errors) == len(expected_errors)
