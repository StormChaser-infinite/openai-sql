import logging
from openai import AzureOpenAI
from azure.storage.blob import ContainerClient 
import pymongo
import datetime as dt


def read_blob(filename: str):
    "Read the SQL Scripts from the blob container"
    try: 
        blobservice = ContainerClient(account_url = "https://kittsqlmodel.blob.core.windows.net", 
                                        credential= "6n/WfghjW+2xAN2h1iCiYxELJPADDC9h5Fr+iLbg+/1kBSoD8eVSeyeStKEyALWyNRE4XEBT8OJY+ASt5EspHQ==",
                                        container_name = "sql-outputs")
        contents = blobservice.get_blob_client(filename).download_blob().readall()
        print(f"Successfully read the file - {filename} from blob container!")
        return contents
    except Exception as e:
        print(f"Fails to connect to the blob container, the error is {e}.")


def create_questions_list(sql: str, filename: str):
    """Create a list of standardised questions which will be push to OpnAI"""
    messages = [f"Summarise the SQL query: {sql}" , 
                f"the SQL query is: {sql}. how the data is flowing?",
                f"the SQL query is: {sql}. what are the inputs?",
                f"the SQL query is: {sql}. what are the outputs?",
                f"the SQL query is: {sql}. what variables are created?",
                f"the SQL query is: {sql}. what are risks running this query?"]
    questions = [f"Summarise the SQL query: {filename}" , 
                f"the SQL query is: {filename}. how the data is flowing?",
                f"the SQL query is: {filename}. what are the inputs?",
                f"the SQL query is: {filename}. what are the outputs?",
                f"the SQL query is: {filename}. what variables are created?",
                f"the SQL query is: {filename}. what are risks running this query?"]

    return messages,questions


def connect_openai(filename: str,promots: list, questions: list, ):
    """connect to Azure openai and run promots to retrive the response"""
    file_reviewed = filename[0:filename.find('.')]
    response = {"queryId": file_reviewed, 
                "review_date": dt.date.today().strftime('%Y-%m-%d'),
                "responses": {}}
    questions_count = 0
    resp = []
    try:
        client = AzureOpenAI(api_version="2023-05-15",
                            azure_endpoint="https://kmrt.openai.azure.com/",
                            api_key= "a95055f384dd4051a696499d00a064a3")
        for m in range(0, len(promots)):
            completion = client.chat.completions.create( model="cd-kmrt-sbx-gpt4",  
                                                        messages=[{"role": "user","content": promots[m]},])
            
            resp.append({"questionId": m,
                          questions[m]: completion.choices[0].message.content})
            questions_count = questions_count + 1

        response.update({"responses": resp})
        
        print(f"Successfully connected to OpenAI API and sent the standardised promots. {questions_count} responses are recieved!")

        return response
    except Exception as e:
        print(f"Connecting to OpenAI API failed because {e}")


def save_reponses(messages: dict):
    """Save the responses to the Cusmos DB"""
    db_name = "sql-model-review"
    collection_name = "kitt-collection"
    try:
        CONNECTION_STRING = "mongodb://kitt-sql-review:mPyy7ouKIKP1qZ6PeE636ByyE158Zcq6ErZLOEdMJUNfupAPcJ445jEw0bPPOMhF3rZn9rg7RRsGACDbuyUrUw==@kitt-sql-review.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@kitt-sql-review@"
        mglient = pymongo.MongoClient(CONNECTION_STRING)
        collection = mglient[db_name][collection_name]
        ## check the collection exists or not
        if "kitt-collection" not in mglient[db_name].list_collection_names():
        # Creates a unsharded collection that uses the DBs shared throughput
            mglient[db_name].command({"customAction": "CreateCollection", "collection": collection_name})
            print("Created collection '{}'.\n".format(collection_name))
        else:
            print("Using collection: '{}'.\n".format(collection_name))
        #create a document
        result = collection.update_one({"name": messages["queryId"]}, 
                                       {"$set": messages}, upsert=True)
        
        print("Upserted {} document with _id {}\n".format(messages["queryId"] ,result.upserted_id))

    except Exception as e:
        print(f"Exporting the responses to Cusmos DB failed because {e}")


def main():
    filename = "GetSizeBand.txt"
    sql_string = read_blob(filename)
    messages,questions = create_questions_list(sql_string, filename)

    response = connect_openai(filename, messages,questions)

    save_reponses(response)




main()






 

 








    


