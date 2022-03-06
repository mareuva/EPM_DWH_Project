import pandas as pd
import os

PATH_TO_ORIGINAL_CSV = "C:/Users/Hanna/Downloads/Iowa_Liquor_Sales.csv"
PATH_TO_FULL_SOUTHERN_SALES_CSV = fr"./output/split_by_district/southern_district_sales.csv"
PATH_TO_FULL_NOTHERN_SALES_CSV = fr"./output/split_by_district/nothern_district_sales.csv"
PATH_TO_CSV_BY_YEARS = fr"./output/split_by_district/by_year/"
PATH_TO_CSV_BY_YEARS_2020_2021 = fr"./output/split_by_district/by_year_2020_2021/"

# splitting the input file into chunks for easier processing and appending data by county from different chucks
for i, chunk in enumerate(pd.read_csv(PATH_TO_ORIGINAL_CSV, chunksize=100000)):
    _ = chunk.groupby("County").apply(lambda x:
                                   x.to_csv(fr"./tmp/county_csv_files/{x.name}.csv", mode='a', index=False, header=False)
                                   if os.path.isfile(fr"./tmp/county_csv_files/{x.name}.csv")
                                   else x.to_csv(fr"./tmp/county_csv_files/{x.name}.csv", index=False))

# appending county csv files by district
county_paths = []
southern_district = ['boone', 'dallas', 'greene', 'guthrie', 'jasper', 'madison', 'marion', 'marshall', 'polk',
                     'poweshiek', 'story', 'warren', 'des moines', 'henry', 'lee', 'louisa', 'van buren', 'audubon',
                     'cass', 'fremont', 'harrison', 'mills', 'montgomery', 'page', 'pottawattamie', 'shelby', 'adair',
                     'adams', 'clarke', 'decatur', 'lucas', 'ringgold', 'taylor', 'union', 'wayne', 'clinton',
                     'johnson', 'muscatine', 'scott', 'washington', 'appanoose', 'davis', 'jefferson', 'keokuk',
                     'mahaska', 'monroe', 'wapello']

nothern_district = ["allamakee", "benton", "black hawk", "bremer", "buchanan", "buena vista", "butler", "calhoun",
                    "carroll", "cedar", "cerro gordo", "cherokee", "chickasaw", "clay", "clayton", "crawford",
                    "delaware", "dickinson", "dubuque", "emmet", "fayette", "floyd", "franklin", "grundy",
                    "hamilton", "hancock", "hardin", "howard", "humboldt", "ida", "iowa", "jackson", "jones", "kossuth",
                    "linn", "lyon", "mitchell", "monona", "o'brien", "osceola", "palo alto", "plymouth", "pocahontas",
                    "sac", "sioux", "tama", "webster", "winnebago", "winneshiek", "woodbury", "worth", "wright"]

for root, subdirectory, files in os.walk('./tmp/county_csv_files/'):
    for file in files:
        df = pd.read_csv(root + file)
        if file.split('.')[0].lower() in southern_district:
            if os.path.isfile(PATH_TO_FULL_SOUTHERN_SALES_CSV):
                # appending to southern_district_sales.csv
                df.to_csv(PATH_TO_FULL_SOUTHERN_SALES_CSV, mode='a', index=False, header=False)
            else:
                # creating southern_district_sales.csv
                df.to_csv(PATH_TO_FULL_SOUTHERN_SALES_CSV, index=False)
        elif file.split('.')[0].lower() in nothern_district:
            if os.path.isfile(PATH_TO_FULL_NOTHERN_SALES_CSV):
                # saving to nothern_district_sales.csv
                df.to_csv(PATH_TO_FULL_NOTHERN_SALES_CSV, mode='a', index=False, header=False)
            else:
                # creating nothern_district_sales.csv
                df.to_csv(PATH_TO_FULL_NOTHERN_SALES_CSV, index=False)
        else: # checking for any dirty data
            print("OOPS! We've missed " + str(file.split('.')[0].lower()))

""" at this stage no data cleaning is performed so we're going to hardcode
and append the rest of the files without any value changes"""
df = pd.read_csv('./tmp/county_csv_files/OBRIEN.csv') #O'Brien
df.to_csv(PATH_TO_FULL_NOTHERN_SALES_CSV, mode='a', index=False, header=False)
df = pd.read_csv('./tmp/county_csv_files/BUENA VIST.csv') #Buena Vista
df.to_csv(PATH_TO_FULL_NOTHERN_SALES_CSV, mode='a', index=False, header=False)
df = pd.read_csv('./tmp/county_csv_files/POTTAWATTA.csv') #Pottawattamie
df.to_csv(PATH_TO_FULL_SOUTHERN_SALES_CSV, mode='a', index=False, header=False)
df = pd.read_csv('./tmp/county_csv_files/CERRO GORD.csv') #Cerro Gordo
df.to_csv(PATH_TO_FULL_NOTHERN_SALES_CSV, mode='a', index=False, header=False)

# splitting resulting csv files by year
for path in [PATH_TO_FULL_SOUTHERN_SALES_CSV, PATH_TO_FULL_NOTHERN_SALES_CSV]:
    df = pd.read_csv(path, parse_dates=['Date'])
    _ = df.groupby(df.Date.dt.year).apply(lambda x: x.to_csv(
            PATH_TO_CSV_BY_YEARS+path.split('/')[-1].replace('.csv', fr"_{x.name}.csv",
                mode='a', index=False, header=False)
        if os.path.isfile(PATH_TO_CSV_BY_YEARS+path.split('/')[-1].replace('.csv', fr"_{x.name}.csv"))
        else x.to_csv(PATH_TO_CSV_BY_YEARS+path.split('/')[-1].replace('.csv', fr"_{x.name}.csv"), index=False)))

# combining csv files for 2020-2021 by districts
for root, directories, files in os.walk(PATH_TO_CSV_BY_YEARS):
    """
    since os.walk() can include to filenames files created during code execution
    it's crucial to save output files outside the directory specified in arguments
    """
    #I'm filtering by year at the very end to make the job easier in case I'll ever need to change the years included
    for file in files:
        if file[-8:-4] == '2020' or file[-8:-4] == '2021':
            df = pd.read_csv(root + file)
            if os.path.isfile(PATH_TO_CSV_BY_YEARS_2020_2021 + file.split('/')[-1].replace(file[-8:-4], "2020_2021")):
                df.to_csv(PATH_TO_CSV_BY_YEARS_2020_2021 + file.split('/')[-1].replace(file[-8:-4], "2020_2021"),
                          mode='a', index=False, header=False)
            else:
                df.to_csv(PATH_TO_CSV_BY_YEARS_2020_2021 + file.split('/')[-1].replace(file[-8:-4], "2020_2021"),
                          index=False)