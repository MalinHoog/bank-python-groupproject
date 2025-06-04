import os
import sys
import pandas as pd
import uuid
from sqlalchemy import create_engine, Column, String, Integer, Numeric, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

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

engine = create_engine("postgresql://postgres:Fo3aex6626@localhost:5432/postgres")
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

try:
    df_customers = pd.read_csv("data/sebank_customer_FINAL.csv")
    print("Customer CSV columns:", df_customers.columns.tolist())

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

    df_transactions = pd.read_csv("data/clean_transactions.csv")
    print("Transactions CSV columns:", df_transactions.columns.tolist())

    for _, row in df_transactions.iterrows():
        row_data = row.to_dict()
        row_data['transaction_id'] = uuid.UUID(row_data['transaction_id'])

        if not session.get(Transaction, row_data["transaction_id"]):
            transaction = Transaction(**row_data)
            session.add(transaction)

    session.commit()

except Exception as e:
    session.rollback()
    print("Ett fel inträffade, ändringar har rullats tillbaka.")
    print(e)

finally:
    session.close()