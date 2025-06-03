import psycopg2
import pandas as pd

# Anslut till PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Fo3aex6626",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()



cursor.execute("""
CREATE TABLE IF NOT EXISTS Transaction (
    transaction_id UUID PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    amount NUMERIC,
    currency TEXT,
    sender_account TEXT,
    receiver_account TEXT,
    sender_country TEXT,
    sender_municipality TEXT,
    receiver_country TEXT,
    receiver_municipality TEXT,
    transaction_type TEXT,
    notes TEXT,
    amount_sek NUMERIC
);
""")
conn.commit()

# Läs in CSV
df = pd.read_csv('***')

# Stoppa in varje rad
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO Transaction (
            transaction_id, timestamp, amount, currency,
            sender_account, receiver_account,
            sender_country, sender_municipality,
            receiver_country, receiver_municipality,
            transaction_type, notes, amount_sek
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s
        );
    """, (
        row['transaction_id'], row['timestamp'], row['amount'], row['currency'],
        row['sender_account'], row['receiver_account'],
        row['sender_country'], row['sender_municipality'],
        row['receiver_country'], row['receiver_municipality'],
        row['transaction_type'], row['notes'], row['amount_sek']
    ))

conn.commit()