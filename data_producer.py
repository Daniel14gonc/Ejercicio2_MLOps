from kafka import KafkaConsumer
import json
import pandas as pd

# Configurar el consumidor de Kafka
consumer = KafkaConsumer('recommendations',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Crear una lista para almacenar los datos
data = []

# Consumir los mensajes de Kafka
for message in consumer:
    data.append(message.value)

    # Salir del bucle después de consumir 100 mensajes para el ejemplo
    if len(data) >= 100:
        break

# Crear un DataFrame con los datos
df = pd.DataFrame(data)

# Guardar el DataFrame en un archivo CSV
df.to_csv('kafka_data.csv', index=False)

print('Datos guardados en kafka_data.csv')