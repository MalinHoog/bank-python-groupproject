import psycopg2
import pandas as pd

# Anslutning till PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="HimeSessan24!",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Skapa customer tabell
cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        customer VARCHAR(100),
        phone VARCHAR(20),
        personnummer VARCHAR(20),
        bankaccount VARCHAR(40),
        street VARCHAR(100),
        postalcode VARCHAR(10),
        city VARCHAR(100)
    );
""")
conn.commit()

# Läs in data från CSV
df = pd.read_csv("data/otherdatafiles/sebank_customer_FINAL.csv")  # byt till korrekt filnamn

# Lägg in rader i databasen
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO customers (customer, phone, personnummer, bankaccount, street, postalcode, city)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, tuple(row))

conn.commit()

# Skapa transactions tabell
cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id UUID PRIMARY KEY,
        timestamp TIMESTAMP,
        amount NUMERIC,
        currency VARCHAR(10),
        sender_account VARCHAR(40),
        receiver_account VARCHAR(40),
        sender_country VARCHAR(100),
        sender_municipality VARCHAR(100),
        receiver_country VARCHAR(100),
        receiver_municipality VARCHAR(100),
        transaction_type VARCHAR(50),
        notes TEXT,
        amount_sek NUMERIC
    );
""")
conn.commit()

# Läs in transactions-data från CSV
df_transactions = pd.read_csv("data\clean_transactions.csv")

for _, row in df_transactions.iterrows():
    transaction_id = row['transaction_id']
    cur.execute("SELECT 1 FROM transactions WHERE transaction_id = %s;", (transaction_id,))
    if not cur.fetchone():
        cur.execute("""
            INSERT INTO transactions (
                transaction_id, timestamp, amount, currency, sender_account, receiver_account,
                sender_country, sender_municipality, receiver_country, receiver_municipality,
                transaction_type, notes, amount_sek
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, tuple(row))

conn.commit()

# Stäng anslutningen
cur.close()
conn.close()

