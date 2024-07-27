from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('recommendations',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         value_serializer=lambda v: json.loads(v.decode('utf-8')))

for message in consumer:
    print(message.value)
