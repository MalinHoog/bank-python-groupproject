import psycopg2
import pandas as pd


"""
# Anslut till PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Fo3aex6626",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Skapa tabeller
cursor.execute("""
CREATE TABLE IF NOT EXISTS Customer (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Address (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES Customer(id),
    street TEXT,
    city TEXT,
    zip_code TEXT
);

CREATE TABLE IF NOT EXISTS Phone (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES Customer(id),
    phone_number TEXT
);

CREATE TABLE IF NOT EXISTS Personnummer (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES Customer(id),
    personnummer TEXT
);

CREATE TABLE IF NOT EXISTS BankAccount (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES Customer(id),
    account_number TEXT
);
""")
conn.commit()

# Läs CSV
df = pd.read_csv('data/sebank_customers_with_accounts.csv')

for _, row in df.iterrows():
    # Lägg in kund
    cursor.execute("INSERT INTO Customer (name) VALUES (%s) RETURNING id;", (row['Customer'],))
    customer_id = cursor.fetchone()[0]

    # Dela upp adress: "Ängsvägen 03, 14010 Gävle"
    address_full = row['Address']
    if ',' in address_full:
        street, zip_city = map(str.strip, address_full.split(',', 1))
        parts = zip_city.split(' ', 1)
        if len(parts) == 2:
            zip_code, city = parts
        else:
            zip_code, city = None, zip_city
    else:
        street, zip_code, city = address_full, None, None

    # Lägg in adress
    cursor.execute("""
        INSERT INTO Address (customer_id, street, city, zip_code) 
        VALUES (%s, %s, %s, %s);
    """, (customer_id, street, city, zip_code))

    # Lägg in telefon
    cursor.execute("""
        INSERT INTO Phone (customer_id, phone_number) 
        VALUES (%s, %s);
    """, (customer_id, row['Phone']))

    # Lägg in personnummer
    cursor.execute("""
        INSERT INTO Personnummer (customer_id, personnummer) 
        VALUES (%s, %s);
    """, (customer_id, row['Personnummer']))

    # Lägg in bankkonto
    cursor.execute("""
        INSERT INTO BankAccount (customer_id, account_number) 
        VALUES (%s, %s);
    """, (customer_id, row['BankAccount']))

conn.commit()
cursor.close()
conn.close()

df = pd.read_csv('data/sebank_customers_with_accounts.csv')

"""


