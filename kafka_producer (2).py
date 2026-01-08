import json 
import time 
import uuid
from kafka import KafkaProducer 
import requests

 
"""
    Creates a Kafka producer and sends employee data
    fetched from a public API to a Kafka topic.
 """


main_topic="datalake"  # Main Kafka topic: valid/successful messages are sent here
DLQ_topic="datalakedlq"# Dead Letter Queue topic: failed messages/errors are sent here
API="https://randomuser.me/api/"

def bulid_data (user_data,message_id) : # Build the final message payload by extracting the fields we need from the API response
    return {
    "message_id" : message_id,
    "name": f"{user_data['name']['first']} {user_data['name']['last']}" ,
     "age" :user_data["dob"]["age"] ,
     "gender" : user_data["gender"] ,
     "country" : user_data["location"]["country"],
     "email"  : user_data["email"],
     "time_msg" :time.time()
    }
def send_kafka(producer , topic, payload, wait_sec=10) : # Send a message to Kafka and wait for an acknowledgement (up to wait_sec seconds)
   x = producer.send(topic,value=payload)
   return x.get(timeout=wait_sec)
def fetch_retry(API, attempts=3, timeout_sec=5, base_sleep=0.5): # Fetch data from the API with retries (handles HTTP errors and network exceptions)
   

    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(API, timeout=timeout_sec)

            if response.status_code == 200:
                return response, None

            last_error = {   # Non-200 HTTP response: store details for debugging/monitoring
                "type": "HTTP_ERROR",
                "status_code": response.status_code,
                "response_text": response.text[:500]
            }

        except requests.RequestException as e:
            last_error = {
                "type": "REQUEST_EXCEPTION",
                "message": str(e)
            }

        if attempt < attempts:
            time.sleep(base_sleep * attempt)

    return None, last_error

def start_producer() :
         producer = KafkaProducer (
            bootstrap_servers= "kafka1:9092" ,
            acks ="all" , 
            retries =5 ,
            value_serializer= lambda v :json.dumps(v).encode("utf-8")
         )   
         for _ in range (10) :
            message_id= str(uuid.uuid4())
            response , err= fetch_retry(API, attempts=3 , timeout_sec=10, base_sleep=1)
            if err is not None :
                dlq_mesg={
                "message_id" : message_id ,
                "err_time": time.time(),
                "error" : err
                }
                send_kafka(producer,DLQ_topic,dlq_mesg)  
                time.sleep(1)
                continue 
            payload =response.json()
            user_data =payload["results"][0]
            employee_data = bulid_data(user_data,message_id)
            print(f"Producing: {employee_data}")
            send_kafka(producer ,main_topic , employee_data)
            time.sleep(1)
         producer.flush()
         producer.close()
if __name__ == '__main__' :
                
 start_producer()                 
                

