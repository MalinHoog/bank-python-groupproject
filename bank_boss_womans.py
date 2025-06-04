import os
import sys
import pandas as pd
import uuid
from sqlalchemy import create_engine, Column, String, Integer, Numeric, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base, sessionmaker

# Deklarera basen för modeller
Base = declarative_base()

# Skapar Customer-tabellen
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    customer = Column(String(100))
    phone = Column(String(20))
    personnummer = Column(String(20))
    bankaccount = Column(String(40))
    street = Column(String(100))
    postalcode = Column(String(10))
    city = Column(String(100))

# Skapar Transaction-tabellen
class Transaction(Base):
    __tablename__ = 'transactions'

    transaction_id = Column(PG_UUID(as_uuid=True), primary_key=True)
    timestamp = Column(TIMESTAMP)
    amount = Column(Numeric)
    currency = Column(String(100))
    sender_account = Column(String(40))
    receiver_account = Column(String(40))
    sender_country = Column(String(100))
    sender_municipality = Column(String(100))
    receiver_country = Column(String(100))
    receiver_municipality = Column(String(100))
    transaction_type = Column(String(50))
    notes = Column(Text)
    amount_sek = Column(Numeric)

# Skapar anslutning till databasen i datagrap 'bank_womans'
engine = create_engine("postgresql://postgres:Fo3aex6626@localhost:5432/bank_boss_womans")
Session = sessionmaker(bind=engine)
session = Session()

# Skapar tabeller i databasen
Base.metadata.create_all(engine)

# Ladda och spara data
try:
    # Läs in kunddata
    df_customers = pd.read_csv("data/customers_clean.csv")
    print("Customer CSV columns:", df_customers.columns.tolist())

    # Säkerställ att alla telefonnummer är strängar och börjar med "0"
    df_customers["Phone"] = df_customers["Phone"].astype(str)
    df_customers["Phone"] = df_customers["Phone"].apply(lambda x: x if x.startswith("0") else "0" + x)

    for _, row in df_customers.iterrows():
        customer_data = {
            "customer": row["Customer"],
            "phone": row["Phone"],
            "personnummer": row["Personnummer"],
            "bankaccount": row["BankAccount"],
            "street": row["Street"],
            "postalcode": row["PostalCode"],
            "city": row["City"]
        }
        customer = Customer(**customer_data)
        session.add(customer)

    # Läs in transaktionsdata
    df_transactions = pd.read_csv("data/clean_transactions.csv")
    print("Transactions CSV columns:", df_transactions.columns.tolist())

    for _, row in df_transactions.iterrows():
        row_data = row.to_dict()
        row_data['transaction_id'] = uuid.UUID(row_data['transaction_id'])

        if not session.get(Transaction, row_data["transaction_id"]):
            transaction = Transaction(**row_data)
            session.add(transaction)

    # Bekräftar ändringarna
    session.commit()

#Skapar en rollback vid fel
except Exception as e:
    session.rollback()
    print("Ett fel inträffade, ändringar har rullats tillbaka.")
    print(e)

finally:
    session.close()
