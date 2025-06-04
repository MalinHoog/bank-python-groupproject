from sqlalchemy import Column, String, Integer, Numeric, Text, TIMESTAMP, UUID
from sqlalchemy.orm import declarative_base

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

    transaction_id = Column(UUID, primary_key=True)
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
