import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
import csv

# Descargar recursos necesarios de nltk
nltk.download('punkt')

df = pd.read_csv('kafka_data.csv')

print(f"Número de registros antes de la limpieza: {len(df)}")

rating_median = df['rating'].median()
df['rating'].fillna(rating_median, inplace=True)

df_cleaned = df.dropna()

print(f"Número de registros después de la limpieza: {len(df_cleaned)}")

df_cleaned['comments_tokenized'] = df_cleaned['comments'].apply(word_tokenize)

df_cleaned.to_csv('cleaned_tokenized_kafka_data.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

print('Archivo cleaned_tokenized_kafka_data.csv generado')