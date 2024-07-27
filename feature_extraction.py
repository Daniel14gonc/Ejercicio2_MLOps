import pandas as pd
import numpy as np
from datetime import datetime
import csv
from sklearn.preprocessing import MinMaxScaler

df = pd.read_csv('cleaned_tokenized_kafka_data.csv')

scaler = MinMaxScaler()
df['rating'] = scaler.fit_transform(df[['rating']])

df['comment_length'] = df['comments'].apply(lambda x: len(x) if isinstance(x, str) else 0)

df['timestamp'] = pd.to_datetime(df['timestamp'])

df['month'] = df['timestamp'].dt.month
df['day_of_week'] = df['timestamp'].dt.dayofweek

df['year'] = df['timestamp'].dt.year
df['day'] = df['timestamp'].dt.day

print(df.head())

df.to_csv('featured_dummy_kafka_data.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

print('File featured_dummy_kafka_data.csv generated')