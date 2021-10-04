import sys
import random
import json
import time
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType

CONNECTION_STRING = "Endpoint=sb://ansseneb01.servicebus.windows.net/;SharedAccessKeyName=RWX;SharedAccessKey=****;EntityPath=eb001"
client = EventHubProducerClient.from_connection_string(CONNECTION_STRING, eventhub_name = "eb001")

i = 0
for i in range(20):
  now = datetime.now()
  eventhub_data_batch = client.create_batch()
  payload = {}
  data_payload = []
  try:       
    source_id = str(i%9)
    period = now.strftime("%d-%m-%YT%H:%M:%SZ")    

    json_payload = json.dumps({'source_id': source_id, 'period': period})
    eventhub_data_batch.add((EventData(json_payload)))
    client.send_batch(eventhub_data_batch)
    time.sleep(5)
    if(i%10 == 0):
      print(str(i))
  except:
    raise
