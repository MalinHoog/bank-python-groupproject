import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from transaction import convert_to_sek

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
csv_path = os.path.join(base_dir, "data", "clean_transactions.csv")
df = pd.read_csv(csv_path)

# Testar att om valutan redan är SEK
def test_convert_sek():
    row = {"amount": 100, "currency": "SEK"}
    assert convert_to_sek(row) == 100
    # Varför vi gör detta är för att kontrollera att konverteringen av SEK till SEK inte ändrar värdet

# Testar konvertering från USD till SEK
def test_convert_usd():
    row = {"amount": 10, "currency": "USD"}
    result = convert_to_sek(row)
    assert result > 100
    # Varför vi gör detta är för att kontrollera så funktionen returnerar ett omvandlat värde

# Testar att kolumnen transaction_id inte innehåller några null-värden
def test_no_null_transaction_id():
    assert df["transaction_id"].isnull().sum() == 0
    # Säkerställer att alla transaktioner har ett ID

# Testar att alla valutakoder i kolumnen currency har exakt 3 stora bokstäver
def test_currency_format():
    assert df["currency"].str.match(r"^[A-Z]{3}$").all()
    # Validera formatet på valutakoder t.ex. SEK, USD, EURO

