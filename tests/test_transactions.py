import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from transaction import convert_to_sek

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
csv_path = os.path.join(base_dir, "data", "clean_transactions.csv")
df = pd.read_csv(csv_path)


def test_convert_sek():
    row = {"amount": 100, "currency": "SEK"}
    assert convert_to_sek(row) == 100

def test_convert_usd():
    row = {"amount": 10, "currency": "USD"}
    result = convert_to_sek(row)
    assert result > 0

def test_no_null_transaction_id():
    assert df["transaction_id"].isnull().sum() == 0

def test_currency_format():
    assert df["currency"].str.match(r"^[A-Z]{3}$").all()

