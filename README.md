# gr-coding-challenge

# once you have python installed from: 
https://www.python.org/downloads/windows/

# must install libraries to connect to mssql: 
python3 -m pip install pyodbc jsonschema

# must attach the following data/log files to a sql server 2019/2022 (express/development edition), otherwise you'll have to run the ddl script (build.sql) to create the tables on your own sql server instance: 
.\CarRental_log.ldf
.\CarRental.mdf

# in order to run you have to run: 
python3 importsession.py path/to/input.json


<!--
the input.json file in your car_rental_updated.pdf is broken!

there is a missing comma in the ending session for ABC123 after the timestamp. 
you also mismatched the session id & timestamp for the ABC456 session's end record. the session id was 
nonsense (or applicable to a different session) and the timestamp wasn't in in a unix epoch datetime standard, as it was stored in the session id by mistake. i included the original input_original.json so you can see for yourself. you should review this immediately before sending this out. 

-->


# items to consider for improvement: 
# 1. enable fast_executemany for pyodbc
# 2. instead use pandas (see commented out section in the library import at the top)


# ideally this should be done in a fashion of loading these json files into a datalake on aws/azure then 
# building out a data warehouse to analyze: 

<!--
-load the *.json files into s3
-use aws glue/glue ETL to transform the data into another S3 folder
-create target table in redshift
-use the redshift copy command to load data into the new table for analysis with appropriate fact/definition tables., 
-->