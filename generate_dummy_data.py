import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Generar datos dummy
np.random.seed(42)  # Para reproducibilidad
num_records = 2000

# Generar timestamps aleatorios en un rango de fechas
start_date = datetime(2023, 1, 1)
timestamps = [start_date + timedelta(days=np.random.randint(0, 180)) for _ in range(num_records)]

# Generar ubicaciones aleatorias
locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
random_locations = [random.choice(locations) for _ in range(num_records)]

# Generar comentarios aleatorios y algunos valores faltantes
comments = ['Good', 'Average', 'Excellent', np.nan, 'Poor', '', 'Could be better', np.nan]
random_comments = [random.choice(comments) for _ in range(num_records)]

# Valor especial para representar valores faltantes en columnas int
missing_value = -1

data = {
    'user_id': np.random.randint(1, 30, num_records),
    'item_id': np.random.randint(1, 15, num_records),
    'rating': np.random.choice([1, 2, 3, 4, 5, np.nan], num_records),  # Ratings con valores faltantes
    'timestamp': timestamps,
    'location': random_locations,
    'comments': random_comments
}

# Introducir algunos valores faltantes y inconsistencias
for _ in range(int(num_records * 0.1)):  # 10% de los registros
    data['user_id'][np.random.randint(0, num_records)] = missing_value
    data['item_id'][np.random.randint(0, num_records)] = missing_value
    data['location'][np.random.randint(0, num_records)] = ''

# Crear un DataFrame
df = pd.DataFrame(data)

# Reemplazar el valor especial por NaN
df.replace(missing_value, np.nan, inplace=True)

# Guardar en un archivo CSV
df.to_csv('kafka_data.csv', index=False)

print('Archivo dummy_kafka_data.csv generado')