import azure.functions as func
from azure.storage.blob import ContainerClient, BlobServiceClient, BlobClient
import logging
import re


check_list = ["create view", "create function", "create procedure"]
connectionstring = "DefaultEndpointsProtocol=https;AccountName=kittsqlmodel;AccountKey=6n/WfghjW+2xAN2h1iCiYxELJPADDC9h5Fr+iLbg+/1kBSoD8eVSeyeStKEyALWyNRE4XEBT8OJY+ASt5EspHQ==;EndpointSuffix=core.windows.net"
input_container = "sql-inputs"
output_container = "sql-outputs"

app = func.FunctionApp()
@app.blob_trigger(arg_name="myblob", 
                  path="sql-inputs/{name}",
                  connection="blobconnectionstring") 


def split_sql_input(myblob: func.InputStream):
    """Split the uploaded SQL file, if it contains more than one SP/Functions/Views then the SQL script will be split into individual txt file"""
    try:
        blobService = BlobServiceClient.from_connection_string(connectionstring)
        blob_client = blobService.get_blob_client(myblob.name[0:myblob.name.find("/")], myblob.name[myblob.name.find("/")+1:])
        try:
            sqllines = blob_client.download_blob().readall().decode('utf-8')
        except: 
            try:
                sqllines = blob_client.download_blob().readall().decode('utf-16')
            except Exception as e:
                print(f"The input file {myblob.name} cannot open!")

        counts = 0
        sql_lines = sqllines.split('\n')
        for i in sql_lines:
            if i.lower().strip().startswith(check_list[0]) or i.lower().strip().startswith(check_list[1]) or i.lower().strip().startswith(check_list[2]):
                counts = counts + 1

        if counts == 0:
            print(f"The file {myblob.name} couldnot find any Stored Procedures, functions or views!" )
        elif counts == 1: 
            print(f'There is only one SP/Function/View in the SQL script, no need to split the file!')
            export_txt_file(sql_lines)  
        else:
            print(f'There are {counts} SPs/Functions/Views in the SLQ Script!')
            indexes = [index for index in range(len(sql_lines)) if sql_lines[index].lower().strip().startswith('SET ANSI_NULLS ON'.lower())]
            if len(indexes) == 0:
                print(f"The file contains SP/Functions/Views, but the sytax might be incorrect!")
            else:
                count_output = 0
                for i in range(0, len(indexes)):
                    if i < len(indexes) - 1:
                        sql = sql_lines[indexes[i]-2:indexes[i+1]-2]
                        export_txt_file(sql)
                        count_output = count_output + 1
                    else:
                        sql = sql_lines[indexes[i]-2:-1]
                        export_txt_file(sql)
                        count_output = count_output + 1
                print(f"There are {count_output} SPs/Functions/Views which were exported into txt files!")
    except Exception as e:
        print(f'The SQL scripts did not split successfully because {e}.')


def export_txt_file(sql_section: list):
    """Exported the split SQL script into txt files """
    try:
        for s in sql_section:
            if s.lower().strip().startswith(check_list[0]) or s.lower().strip().startswith(check_list[1]) or s.lower().strip().startswith(check_list[2]):
                end_index = [e for e, n in enumerate(list(s)) if n == ']'][1]
                file_name = re.sub("[\W_]+"," ", ' '.join([t.lower() for t in s[0:end_index].strip().split(" ")])).replace(" ", "_")
                
        output_file = file_name + '.txt'
        blobService = BlobServiceClient.from_connection_string(connectionstring)
        blob_client = blobService.get_blob_client(output_container, output_file)

        sqloutput = ''.join(sql_section)
        blob_client.upload_blob(sqloutput, overwrite=True)

    except Exception as e:
        print(f'The name cannot be found for the SP/Function/View! Because {e}')


def sqlquerysplit(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    split_sql_input(myblob)







