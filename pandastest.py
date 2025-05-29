import pandas as pd
# sender_country,sender_municipality,receiver_country,receiver_municipality,transaction_type,notes

# Läser in CSV-filen
df = pd.read_csv("data/transactions.csv")
df.index += 2

# Lista över kolumner att kontrollera
kolumner = [
    "sender_country",
    "sender_municipality",
    "receiver_country",
    "receiver_municipality",
    "transaction_type"
]

# Filtrera rader där minst en av dessa kolumner har saknat värde
filtered_df = df[df[kolumner].isnull().any(axis=1)]

# Visa resultatet
print("Rader med saknade värden i de angivna kolumnerna:")
print(filtered_df)




