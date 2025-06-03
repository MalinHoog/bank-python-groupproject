import pandas as pd
from datetime import datetime
import random
import warnings
import pycountry_convert as pc

warnings.filterwarnings("ignore", message="`result_format` configured at the Validator-level*")

# Ladda datan
df_customers = pd.read_csv("./data/sebank_customers_with_accounts.csv")
df_transactions = pd.read_csv("./data/transactions.csv")
df_customers.index += 2
df_transactions.index += 2
df_transactions["timestamp"] = pd.to_datetime(df_transactions["timestamp"])

# -----------------------------
# TRANSAKTIONSANALYS (df_transactions)
# -----------------------------

# Visa alla olika valutor
unique_currencies = df_transactions["currency"].dropna().str.strip().str.upper().unique()
unique_currencies = sorted(unique_currencies)
print(f"Amount of different currencies: {len(unique_currencies)}")
print(unique_currencies)

# Self-transfers (avsändare och mottagare = samma konto)
same_account = df_transactions[df_transactions["sender_account"] == df_transactions["receiver_account"]]
print(f"\nSelf-transfers: {len(same_account)} transaktioner")
print(same_account)

# Rader med null-värden (exkl. notes)
rows_with_nulls = df_transactions[df_transactions.drop(columns=["notes"]).isnull().any(axis=1)]
print(f"\nRows with nulls (not notes): {len(rows_with_nulls)}")
print(rows_with_nulls)

# Transaktioner utan notes
missing_notes = df_transactions[df_transactions["notes"].isnull() | (df_transactions["notes"].str.strip() == "")]
print(f"\nTransactions without notes: {len(missing_notes)}")
print(missing_notes)

# Internationella transaktioner (ej Sverige)
non_swedish = df_transactions[
    (df_transactions["sender_country"].str.lower() != "sweden") |
    (df_transactions["receiver_country"].str.lower() != "sweden")
]
print(f"\nInternational transactions: {len(non_swedish)}")
print(non_swedish)

# Transaktioner där valutan inte är SEK
non_sek_transactions = df_transactions[df_transactions["currency"].str.upper() != "SEK"]
print(f"\nTransactions that are not SEK: {len(non_sek_transactions)}")
print(non_sek_transactions)

# Smurfing (många små transaktioner)
small_transactions = df_transactions[df_transactions["amount"] < 1000]
smurfing_counts = small_transactions.groupby("receiver_account")["sender_account"].nunique().reset_index()
smurfing_candidates = smurfing_counts[smurfing_counts["sender_account"] > 5]
smurfing_transactions = small_transactions[small_transactions["receiver_account"].isin(smurfing_candidates["receiver_account"])]
print(f"\nSmurfing transactions: {len(smurfing_transactions)}")
print(smurfing_transactions)

# Nattliga transaktioner (00–05)
df_transactions["hour"] = df_transactions["timestamp"].dt.hour
night_transactions = df_transactions[(df_transactions["hour"] >= 0) & (df_transactions["hour"] <= 5)]
print(f"\nNightly transactions (00–05): {len(night_transactions)}")
print(night_transactions)

# Upprepade transaktioner mellan samma konton
repeated_pairs = df_transactions.groupby(["sender_account", "receiver_account"]).size().reset_index(name="count")
suspicious_pairs = repeated_pairs[repeated_pairs["count"] > 2]
print(f"\nRepeated transactions (> 2): {len(suspicious_pairs)}")
print(suspicious_pairs)

# Structured transactions
duplicate_amounts = df_transactions.groupby("amount").size().reset_index(name="count")
common_amounts = duplicate_amounts[duplicate_amounts["count"] > 5]
structured_transactions = df_transactions[df_transactions["amount"].isin(common_amounts["amount"])]
print(f"\nStructured transactions: {len(structured_transactions)}")
print(structured_transactions)

# Extrahera årtal och månad
df_transactions["year"] = df_transactions["timestamp"].dt.year
df_transactions["month"] = df_transactions["timestamp"].dt.month
print(f"Extracted year and month")
print(df_transactions[["timestamp", "year", "month"]].head())

# Outgoing och Incoming
outgoing_transactions = df_transactions[df_transactions["transaction_type"].str.lower() == "outgoing"]
incoming_transactions = df_transactions[df_transactions["transaction_type"].str.lower() == "incoming"]

# Kontinenter
def get_continent(country):
    try:
        code = pc.country_name_to_country_alpha2(country)
        cont_code = pc.country_alpha2_to_continent_code(code)
        return pc.convert_continent_code_to_continent_name(cont_code)
    except:
        return "Unknown"

df_transactions["sender_continent"] = df_transactions["sender_country"].apply(get_continent)
df_transactions["receiver_continent"] = df_transactions["receiver_country"].apply(get_continent)
print(f"Continent")
print(df_transactions[["sender_continent", "receiver_continent"]].head(20))

# Konvertering till SEK
conversion_rates = {
    "DKK": 1.5527, "EUR": 11.6084, "GBP": 13.7361, "JPY": 0.0660, "NOK": 0.9830,
    "RMB": 1.4368, "USD": 10.2930, "ZAR": 0.5792, "ZMW": 0.3584, "AUD": 6.9731, "BGN": 5.8452, "BRL": 1.9697, "CAD": 7.7143, "CHF": 12.0045, "CNY": 1.4680, "CZK": 0.4550, "HKD": 1.3536, "HUF": 0.0289, "IDR": 0.0007, "ILS": 2.8540, "INR": 0.1262, "ISK": 0.0766, "KRW": 0.0078, "MXN": 0.5796, "MYR": 2.3127, "NZD": 6.3946, "PHP": 0.1844, "PLN": 2.6551, "RON": 2.2981, "SGD": 7.9076, "THB": 0.2997, "TRY": 0.3220
}

def convert_to_sek(row):
    currency = str(row["currency"]).upper()
    if currency == "SEK":
        return row["amount"]
    return round(row["amount"] * conversion_rates.get(currency, 0), 2)

df_transactions["amount_sek"] = df_transactions.apply(convert_to_sek, axis=1)
print(f"\nConverted currencies to SEK:")
print(df_transactions[["currency", "amount", "amount_sek"]].tail(20))

# Skapa low risk currencies
low_risk_currencies = {"SEK", "JPY"}

# Skapa moderate risk currencies
moderate_risk_currencies = {"EUR", "GBP", "USD", "NOK", "DKK", "CHF", "SGD", "HKD", "AUD", "NZD", "CAD", "HUF", "CZK", "RON", "PLN", "BGN", "KRW", "ISK", "MYR", "ILS"}

# Skapa high risk currencies
high_risk_currencies = {"ZMW", "ZAR", "TRY", "BRL", "IDR", "MXN", "PHP", "INR", "THB", "CNY", "RMB"}

def classify_currency_risk(currency):
    c = str(currency).upper()
    if c in high_risk_currencies:
        return "High risk"
    elif c in moderate_risk_currencies:
        return "Moderate risk"
    elif c in low_risk_currencies:
        return "Low risk"
    else:
        return "Unknown risk"

df_transactions["currency_risk"] = df_transactions["currency"].apply(classify_currency_risk)

# Filtrera utifrån risk
high_risk_currency_transactions = df_transactions[df_transactions["currency_risk"] == "High"]
moderate_risk_currency_transactions = df_transactions[df_transactions["currency_risk"] == "Moderate"]
low_risk_currency_transactions = df_transactions[df_transactions["currency_risk"] == "Low"]

print(f"\nHigh risk-transaktioner: {len(high_risk_currency_transactions)}")
print(high_risk_currency_transactions[["transaction_id", "currency", "amount", "currency_risk"]].head(10))
print(f"\nModerate risk-transaktioner: {len(moderate_risk_currency_transactions)}")
print(moderate_risk_currency_transactions[["transaction_id", "currency", "amount", "currency_risk"]].head(10))
print(f"\nLow risk-transaktioner: {len(low_risk_currency_transactions)}")
print(low_risk_currency_transactions[["transaction_id", "currency", "amount", "currency_risk"]].head(10))

# -----------------------------
# KUNDANALYS
# -----------------------------
df_customers["Phone"] = df_customers["Phone"].astype(str)
df_customers["Phone"] = df_customers["Phone"].str.replace(r"^\+46\s?\(0\)", "0", regex=True)
df_customers["Phone"] = df_customers["Phone"].str.replace(r"\s+", "", regex=True).str.replace("-", "")
# KOLLA TELEFONNUMMER OM DET FINNS SOM EJ ÄR +46!!!!!!!!!!!!!!!!!!!
print(f"Converted Swedish phone numbers to same structure")
print(df_customers[["Phone"]])

# Postort
df_customers[["Street", "ZipCity"]] = df_customers["Address"].str.split(",", n=1, expand=True)
df_customers["ZipCity"] = df_customers["ZipCity"].str.strip()
df_customers[["Postnummer", "Postort"]] = df_customers["ZipCity"].str.extract(r"(\d{5})\s+(.+)")
print(f"Extracted zip and city")
print(df_customers[["Postnummer", "Postort", "Street"]].head(10))

# Konton per person
df_customers["KontonPerPerson"] = df_customers.groupby("Personnummer")["BankAccount"].transform("count")
print(f"Show amount of customers accounts")
print(df_customers[["KontonPerPerson"]])

# Saldo och PIN
unique_accounts = df_customers["BankAccount"].unique()
df_customers["saldo"] = df_customers["BankAccount"].map({a: round(random.uniform(0, 1e7), 2) for a in unique_accounts})
df_customers["PIN"] = df_customers["BankAccount"].map({a: f"{random.randint(0,9999):04}" for a in unique_accounts})
print(f"Get PIN and saldo")
print(df_customers[["PIN", "saldo"]])

# Kön, år, månad

def extract_gender(pnr):
    try:
        return "Kvinna" if int(pnr.split("-")[-1][2]) % 2 == 0 else "Man"
    except:
        return "Okänd"

def extract_birthyear(pnr):
    try:
        digits = pnr.split("-")[0]
        year = int(digits[:2])
        century = 1900 if year > (datetime.now().year % 100) else 2000
        return century + year
    except:
        return None

def extract_birthmonth(pnr):
    try:
        digits = pnr.split("-")[0]
        return int(digits[2:4])
    except:
        return None

df_customers["Gender"] = df_customers["Personnummer"].apply(extract_gender)
df_customers["BirthYear"] = df_customers["Personnummer"].apply(extract_birthyear)
df_customers["Month"] = df_customers["Personnummer"].apply(extract_birthmonth)
df_customers["Age"] = datetime.now().year - df_customers["BirthYear"]
print(f"Extract birth year, month, age and gender")
print(df_customers[["Gender", "BirthYear", "Month", "Age"]])

# Validering

df_customers["is_valid_account"] = df_customers["BankAccount"].str.startswith("SE8902")
df_customers["is_valid_age"] = df_customers["Age"] >= 18

df_customers["is_valid"] = df_customers["is_valid_account"] & df_customers["is_valid_age"]
df_customers["error_reason"] = df_customers.apply(
    lambda row: ", ".join(filter(None, [
        "Ogiltigt kontonummer" if not row["is_valid_account"] else "",
        "Under 18 år" if not row["is_valid_age"] else ""
    ])), axis=1
)
print(f"Show valid accounts")
print(df_customers[["error_reason", "is_valid_account", "is_valid_age"]].tail(50))

# Leta upp kontoägare baserat på personnummer                                                                           
account_to_pnr = df_customers.set_index("BankAccount")["Personnummer"].to_dict()                                        
                                                                                                                        
df_transactions["sender_pnr"] = df_transactions["sender_account"].map(account_to_pnr)                                   
df_transactions["receiver_pnr"] = df_transactions["receiver_account"].map(account_to_pnr)                               
                                                                                                                        
internal_same_person = df_transactions[                                                                                 
    (df_transactions["sender_account"] != df_transactions["receiver_account"]) &                                        
    (df_transactions["sender_pnr"].notnull()) &                                                                         
    (df_transactions["sender_pnr"] == df_transactions["receiver_pnr"])                                                  
]                                                                                                                       
                                                                                                                        
print(f"\nInternal transactions (different bank accounts but same account holder): {len(internal_same_person)}")        
print(internal_same_person[[                                                                                            
    "transaction_id", "sender_account", "receiver_account", "sender_pnr", "receiver_pnr", "amount"                      
]].head(10))                                                                                                            

# Skapa datumkolumn
df_transactions["date"] = df_transactions["timestamp"].dt.date

# Räkna antal transaktioner per avsändare per dag
daily_transactions_counts = df_transactions.groupby(["sender_account", "date"]).size().reset_index(name="transactions_count")

# Filtrera konton med fler än 5 transaktioner på samma dag
frequent_daily_senders = daily_transactions_counts[daily_transactions_counts["transactions_count"] > 5]

print(f"\n Accounts with >5 transactions in 24h: {len(frequent_daily_senders)}")
print(frequent_daily_senders.head(10))

# Sortera efter konto och tid
df_transactions_sorted = df_transactions.sort_values(by=["sender_account", "timestamp"])

# Skapa en DataFrame med endast mottagningar (incoming)
incoming = df_transactions[df_transactions["transaction_type"].str.lower() == "incoming"].copy()
incoming = incoming[["receiver_account", "timestamp", "transaction_id"]].rename(
    columns={"receiver_account": "account", "timestamp": "received_time", "transaction_id": "incoming_id"}
)

# Skapa en DataFrame med endast utgående (outgoing)
outgoing = df_transactions[df_transactions["transaction_type"].str.lower() == "outgoing"].copy()
outgoing = outgoing[["sender_account", "timestamp", "transaction_id"]].rename(
    columns={"sender_account": "account", "timestamp": "sent_time", "transaction_id": "outgoing_id"}
)

# Mergar på konto
merged = pd.merge(incoming, outgoing, on="account")

# Beräkna tidsskillnad
merged["time_diff_minutes"] = (merged["sent_time"] - merged["received_time"]).dt.total_seconds() / 60

# Filtrera: skickat inom 1 timme efter mottagning, men inte innan mottagning
fast_pass_through = merged[(merged["time_diff_minutes"] > 0) & (merged["time_diff_minutes"] <= 30)]

print(f"\nTransactions where account makes another transactions in 30 minutes: {len(fast_pass_through)}")
print(fast_pass_through[["account", "received_time", "sent_time", "time_diff_minutes"]].head(10))

