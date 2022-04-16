import pandas as pd
import os


def import_data_from_local(reporting_entity, 
                           path_to_data = '/Users/christyjlewis/Google Drive/My Drive/Climate TRACE /Metamodeling/data/raw_data/',
                           verbose=True):
    """take reporting entity name from cleaner and export a dictionary with original
    filename as key and data as value

    files input must have the following naming structures to be successfuly inported:

    inventory-name_file-description_YYYYMMDD """

    data = {}
    for file in os.listdir(path_to_data):
        inventory = file.split('_')[0]
        file_info = file.split('.')[0].strip(inventory).lstrip('_')
        if inventory == reporting_entity:
            if verbose:
                print(f'Importing {file}')
            if file.endswith('.csv'):
                df = pd.read_csv(os.path.join(path_to_data,file))
                data[file_info] = df
            elif file.endswith('.xlsx') | file.endswith('.xls'):
                f = pd.ExcelFile(path_to_data + file, engine='openpyxl')
                sheet_names = f.sheet_names
                sheets = {sheet: f.parse(sheet_name=sheet) for sheet in sheet_names}
                data.update(sheets)
    return data

