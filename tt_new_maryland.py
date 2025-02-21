import csv
import os
from pathlib import Path
from datetime import date
from spanish_utils import Spanish_Utils
import pandas as pd
import math
import uuid
from string import capwords

BaseDir = Path(__file__).resolve().parent

# Sort by Last Name
un_sorted_files = os.listdir(BaseDir)
if not os.path.exists("sortBySpecialityLastName"):
    os.mkdir("sortBySpecialityLastName")
for un_file in sorted(un_sorted_files):
    if '.csv' in un_file and 'output' not in un_file and 'pcp_assigned' not in un_file and 'maryland' not in un_file:
        encodings = ['utf-8', 'ISO-8859-1', 'latin1']
        for encoding in encodings:
            try:
                df = pd.read_csv(un_file, encoding=encoding)
                break  # If successful, break out of the loop
            except UnicodeDecodeError:
                print(f"Failed to decode using {encoding} encoding. Trying another encoding. {un_file}")
        column_to_sort = ['Specialty', 'Group Name', 'Last Name']
        df.sort_values(by=column_to_sort, inplace=True)
        df.to_csv(rf"{BaseDir}/sortBySpecialityLastName/{un_file}", index=False)


# Asssign county

def assign_county(response, county):
    county_list = []
    for resp in response:
        resp['County'] = county
        county_list.append(resp)
    return county_list


def assign_new_speciality(response):
    new_speciality = []
    for resp in response:
        if not isinstance(resp['Specialty'], str):
            continue
        if isinstance(resp['Specialty'], str) and resp['Specialty'].strip().replace(';',
                                                                                    ' - ') in Spanish_Utils.add_spanish.keys():
            resp['Specialty'] = Spanish_Utils.add_spanish[resp['Specialty'].strip().replace(';', ' - ')]
        if isinstance(resp['ProviderType'], str) and resp[
            'ProviderType'].strip() in Spanish_Utils.provider_type_dict.keys():
            resp['ProviderType'] = Spanish_Utils.provider_type_dict[resp['ProviderType'].strip()]
        resp['City'] = capwords(resp['City'])
        resp['Ethnicity'] = 'Unknown' if not isinstance(resp['Ethnicity'], str) else resp['Ethnicity']
        resp['Race'] = 'Unknown' if not isinstance(resp['Race'], str) else resp['Race']
        if (str(resp['Group Name']) == str(resp['Last Name'])) and (
                resp['First Name'] == '' or isinstance(resp['First Name'], float)):
            resp['First Name'] = 'NULL'
            resp['Last Name'] = 'NULL'
        resp['id'] = str(uuid.uuid4())
        new_speciality.append(resp)
    return new_speciality


files = os.listdir(rf"{BaseDir}/sortBySpecialityLastName")
speciality_last_name = rf"{BaseDir}/sortBySpecialityLastName"
if not os.path.exists(f"{BaseDir}/sorted_csv"):
    os.mkdir(f"{BaseDir}/sorted_csv")

for file in files:
    county = file.split('/')[-1].replace('Test.csv', '')
    csv_file = pd.read_csv(os.path.join(speciality_last_name, file))

    # Correctly convert DataFrame to list of dictionaries
    data_dict = csv_file.to_dict(orient='records')

    # Process the data
    data_after_county = assign_county(data_dict, county)
    data_after_speciality = assign_new_speciality(data_after_county)
    # Save the processed data
    new_save_path = rf"{BaseDir}/sorted_csv/{county}.csv"
    with open(new_save_path, 'w', newline='') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=data_after_speciality[0].keys())
        writer.writeheader()
        writer.writerows(data_after_speciality)
    print(f"County assigned to {file}")
    os.remove(os.path.join(speciality_last_name, file))


def replace_nan_with_null(response):
    nan_with_null = []
    for resp in response:
        for k, v in resp.items():
            if (isinstance(v, float) and math.isnan(v)) or v == 'nan':
                resp[k] = 'NULL'
            if (isinstance(v, str)):
                if not any(char.isalnum() for char in v):
                    resp[k] = 'NULL'
        nan_with_null.append(resp)
    return nan_with_null


files = os.listdir(rf"{BaseDir}/sorted_csv")
output_file = f'maryland-{date.today()}.csv'
provider_types = ['Primary Care Provider / Proveedores de atención primaria',
                  'Specialist Provider / Proveedores especialistas',
                  'Ancillary Provider / Proveedores auxiliares',
                  'Vision Provider / Proveedor de visión',
                  'Pharmacy / Farmacia'
                  ]
exists = False
speciality_assigned_path = rf"{BaseDir}/sorted_csv"
new_save_path = rf"{BaseDir}/{output_file}"
done_uuids = []
for file in files:
    encodings = ['utf-8', 'ISO-8859-1', 'latin1']
    for encoding in encodings:
        try:
            csv_file = pd.read_csv(os.path.join(speciality_assigned_path, file), encoding=encoding)
            print(f"Successfully decoded using {encoding} encoding. {file}")
            break
        except UnicodeDecodeError:
            print(f"Failed to decode using {encoding} encoding. Trying another encoding. {file}")
    data_dict = csv_file.to_dict(orient='records')
    data_after_nan = replace_nan_with_null(data_dict)
    for provider_type in provider_types:
        data = list(filter(lambda x: x['ProviderType'] == provider_type, data_after_nan))
        for d2 in data:
            if d2['id'] in done_uuids:
                continue
            data_to_write = list(
                filter(lambda x: x['AddressLine1'] == d2['AddressLine1'] and x['Specialty'] == d2['Specialty']
                                 and x['Group Name'] == d2['Group Name'], data))
            if data_to_write:
                for d in data_to_write:
                    done_uuids.append(d['id'])
            else:
                data_to_write = [d2]
                done_uuids.append(d2['id'])
            if os.path.exists(new_save_path):
                exists = True
            with open(new_save_path, 'a+', newline='') as output_file:
                writer = csv.DictWriter(output_file, fieldnames=data_to_write[0].keys())
                if not exists:
                    writer.writeheader()
                writer.writerows(data_to_write)

    os.remove(os.path.join(speciality_assigned_path, file))
