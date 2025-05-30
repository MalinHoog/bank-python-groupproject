import pandas as pd
import warnings
warnings.filterwarnings("ignore", message="`result_format` configured at the Validator-level*")

# Ladda datan
df_customers = pd.read_csv("./data/sebank_customers_with_accounts.csv")
df_transactions = pd.read_csv("./data/transactions.csv")
df_customers.index += 2
df_transactions.index += 2
df_transactions["timestamp"] = pd.to_datetime(df_transactions["timestamp"])

# -----------------------------
# KUNDANALYS (df_customers)
# -----------------------------

# Personer med fler än ett konto
multi_account_holders = df_customers.groupby("Personnummer").size().reset_index(name="AntalKonton")
multi_account_holders = multi_account_holders[multi_account_holders["AntalKonton"] > 1]
multi_account_details = df_customers[df_customers["Personnummer"].isin(multi_account_holders["Personnummer"])]
print(f"\nPersoner med fler än ett konto: {len(multi_account_holders)} personer")
print(multi_account_details)

# -----------------------------
# TRANSAKTIONSANALYS (df_transactions)
# -----------------------------

# Self-transfers (avsändare och mottagare = samma konto)
same_account = df_transactions[df_transactions["sender_account"] == df_transactions["receiver_account"]]
print(f"\nSelf-transfers: {len(same_account)} transaktioner")
print(same_account)

# Rader med null-värden (exkl. notes)
rows_with_nulls = df_transactions[df_transactions.drop(columns=["notes"]).isnull().any(axis=1)]
print(f"\nRader med null-värden (förutom notes): {len(rows_with_nulls)}")
print(rows_with_nulls)

# Transaktioner utan notes
missing_notes = df_transactions[df_transactions["notes"].isnull() | (df_transactions["notes"].str.strip() == "")]
print(f"\nTransaktioner utan notes: {len(missing_notes)}")
print(missing_notes)

# Internationella transaktioner (ej Sverige)
non_swedish = df_transactions[
    (df_transactions["sender_country"].str.lower() != "sweden") |
    (df_transactions["receiver_country"].str.lower() != "sweden")
]
print(f"\nInternationella transaktioner: {len(non_swedish)}")
print(non_swedish)

# Upprepade transaktioner mellan samma konton
repeated_pairs = df_transactions.groupby(["sender_account", "receiver_account"]).size().reset_index(name="count")
suspicious_pairs = repeated_pairs[repeated_pairs["count"] > 2]
print(f"\nUpprepade transaktioner (över 2): {len(suspicious_pairs)}")
print(suspicious_pairs)

# Många transaktioner med exakt samma belopp (structured)
duplicate_amounts = df_transactions.groupby("amount").size().reset_index(name="count")
common_amounts = duplicate_amounts[duplicate_amounts["count"] > 5]
structured_transactions = df_transactions[df_transactions["amount"].isin(common_amounts["amount"])]
print(f"\nStructured transactions: {len(structured_transactions)}")
print(structured_transactions)

# Smurfing (många små transaktioner till en mottagare från olika avsändare)
small_transactions = df_transactions[df_transactions["amount"] < 1000]
smurfing_counts = small_transactions.groupby("receiver_account")["sender_account"].nunique().reset_index()
smurfing_candidates = smurfing_counts[smurfing_counts["sender_account"] > 5]
smurfing_transactions = small_transactions[small_transactions["receiver_account"].isin(smurfing_candidates["receiver_account"])]
print(f"\nSmurfing-transaktioner: {len(smurfing_transactions)}")
print(smurfing_transactions)

# Nattliga transaktioner (00–05)
df_transactions["hour"] = df_transactions["timestamp"].dt.hour
night_transactions = df_transactions[(df_transactions["hour"] >= 0) & (df_transactions["hour"] <= 5)]
print(f"\nNattliga transaktioner (00–05): {len(night_transactions)}")
print(night_transactions)

df_customers["Phone_original"] = df_customers["Phone"]

# Standardisera telefonnummer
df_customers["Phone"] = (
    df_customers["Phone"]
    .astype(str)
    .str.replace(r"^\+46\s?\(0\)", "0", regex=True)  # Ersätt +46 (0) i början med 0
    .str.replace(r"\s+", "", regex=True)             # Ta bort alla mellanslag
    .str.replace("-", "", regex=True)                # Ta bort bindestreck
)
print(df_customers[["Phone_original", "Phone"]].head(10))

# Postnummer och postort
df_customers[["Street", "ZipCity"]] = df_customers["Address"].str.split(",", n=1, expand=True)
df_customers["ZipCity"] = df_customers["ZipCity"].str.strip()
df_customers[["Postnummer", "Postort"]] = df_customers["ZipCity"].str.extract(r"(\d{5})\s+(.+)")
print(df_customers[["Postnummer", "Postort"]])

import pandas as pd
from datetime import datetime

# Läs in data
df = pd.read_csv("./data/sebank_customers_with_accounts.csv")

# Funktion för att extrahera födelseår
def extract_birthyear(pnr):
    try:
        digits = pnr.split("-")[0]
        if len(digits) == 6:
            year = int(digits[:2])
            current_year = datetime.now().year % 100
            century = 1900 if year > current_year else 2000
            return century + year
        elif len(digits) == 8:
            return int(digits[:4])
        else:
            return None
    except:
        return None

# Skapa kolumnen 'BirthYear'
df_customers["BirthYear"] = df_customers["Personnummer"].apply(extract_birthyear)
print(df_customers[["Customer", "Personnummer", "BirthYear"]].head())
