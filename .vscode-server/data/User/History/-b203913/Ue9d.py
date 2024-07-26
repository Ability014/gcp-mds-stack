# pip install pandas
# pip install sqlalchemy
# pip3 install psycopg2 
# Install the packages above in your virtual environment if you don't have them. For pyscopg2, you can install its corresponding binary instead
import pandas as pd
from sqlalchemy import create_engine
import os
import re

os.chdir("./jaffle-data")

# create connection to your postgres db
engine = create_engine("postgresql+psycopg2://postgres:password1234@localhost:5433/postgres")

jaffle_data = os.listdir() # This assigns the list data objects available in your directory to the variable on the left
for data in jaffle_data: # loops through each data objects (CSVs)
    df = pd.read_csv(data) # reads the data as pandas dataframe
    print(df.head()) # prints the first five rows of the data
    print(f'Loading {data[:-4]} into postgres') # prints a log to the console to show what the program is doing at the moment
    """
        The code below loads each of the data object to the specified location as specified in your configuration
    """
    df.to_sql(f'jaffle_{data[:-4]}', \
        engine, \
        schema = 'jaffle', \
        if_exists = 'append', \
        index = False)
    
print('Loaded all data objects to the db')