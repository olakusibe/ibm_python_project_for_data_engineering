import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime
import sys as sys
import os as os

tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored

### DOWNLOAD FROM URL AND UNZIP ###

# About The Data
# The file dealership_data contains CSV, JSON, and XML files for used car data 
# which contain features named car_model, year_of_manufacture, price, and fuel.

# run the shell command to downlaod the zip file
# > !wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip

# run the shell command to unzip the download
# > !unzip datasource.zip -d dealership_data

zip_download_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip'
zip_filename = 'datasource.zip'
unzip_dir = 'dealership_data'

if sys.platform == 'win32': # run windows cmd commands    
    os.system('wget ' + zip_download_url)
    os.system('mkdir ' + unzip_dir)
    os.system('tar -xf ' + zip_filename + ' --directory ' + unzip_dir)    
elif sys.platform == 'linux': # run linux bash commands (NOT TESTED)
    os.system('wget ' + zip_download_url)
    os.system('unzip ' + zip_filename + ' -d ' + unzip_dir)
else: # just want to believe linux commands will work for other OS (e.g 'macOS') (NOT TESTED)
    os.system('wget ' + zip_download_url)
    os.system('unzip ' + zip_filename + ' -d ' + unzip_dir)

### END DOWNLOAD FROM URL AND UNZIP ###


### EXTRACT ###

# Add the CSV extract function below
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# Add the JSON extract function below
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

# Add the XML extract function below, it is the same as the xml extract function above but the column names need to be renamed.
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    
    car_model = []
    year_of_manufacture = []
    price = []
    fuel = []

    for person in root:
        car_model.append(person.find("car_model").text)        
        year_of_manufacture.append(int(person.find("year_of_manufacture").text))
        price.append(float(person.find("price").text))
        fuel.append(person.find("fuel").text)

    data = {
        "car_model":car_model, 
        "year_of_manufacture":year_of_manufacture,
        "price":price,
        "fuel":fuel
        }
    df = pd.DataFrame(data)
    dataframe = pd.concat([dataframe, df], ignore_index = True)                

    return dataframe

# Add collection of all the extract function (csv, json and xml) in one place
def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("dealership_data/*.csv"):
        #extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True) 'Deprecated
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob("dealership_data/*.json"):
        #extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True) 'Deprecated
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("dealership_data/*.xml"):
        #extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)  'Deprecated
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)
        
    return extracted_data

### END EXTRACT ###


### TRANSFORM ###

# Add the transform function below
def transform(data):
    data["price"] = round(data.price,2)
    return data

### END TRANSFORM ###


### LOAD ###

# Add the load function below
def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

### END LOAD ###


### LOG ###

# Add the log function below
def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y' #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')

### END LOG ###


### EXECUTE ETL Process ###

# Log that you have started the ETL process
log("starting ETL process")

# Log that you have started the Extract step
log("starting Extract process")

# Call the Extract function
extracted_data = extract()
print(extracted_data)

# normalize/convert the data type in these columns to a specific type
# especialy if there is a mix type to avoid AttributeError and TypeError
convert_dict = {
    'year_of_manufacture': int,
    'price': float
    }

extracted_data = extracted_data.astype(convert_dict)

# Log that you have completed the Extract step
log("end of Extract process")

# Log that you have started the Transform step
log("starting Transform process")

# Call the Transform function
transformed_data = transform(extracted_data)
print(transformed_data)

# Log that you have completed the Transform step
log("end of Transform process")

# Log that you have started the Load step
log("starting of Load process")

# Call the Load function
load(targetfile,transformed_data)

# Log that you have completed the Load step
log("end of Load process")

# Log that you have completed the ETL process
log("End of ETL process")

### END EXECUTE ETL Process ###