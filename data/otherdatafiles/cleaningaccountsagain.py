import pandas as pd
import re

"""

df = pd.read_csv("data\sebank_customers_superclean.csv")

# Lägg till en 0 i början av varje telefonnummer (som sträng)
df["Phone"] = "0" + df["Phone"].astype(str).str.strip()

# Spara till ny fil
df.to_csv("sebank_customers_superclean_phonefixed.csv", index=False)

"""

"""
# Läs in filen
df = pd.read_csv("sebank_customers_superclean_phonefixed.csv")

# Funktion som korrigerar gatunumret
def korrigera_street(adress):
    # Matchar t.ex. "Parkvägen 09", "Skolgatan 003" men inte "Parkvägen 0"
    match = re.match(r"^(.*\s)0+(\d+)$", adress.strip())
    if match:
        # Behåll gatunamnet + den faktiska siffran (utan ledande nollor)
        return match.group(1) + match.group(2)
    else:
        return adress

# Använd funktionen på Street-kolumnen
df["Street"] = df["Street"].astype(str).apply(korrigera_street)

# Spara till ny fil
df.to_csv("sebank_customers_finalfinally.csv", index=False)
"""

"""
# Läs in filen med adresser som har gatunummer 00
df = pd.read_csv("data/otherdatafiles/adresser_med_gatunummer_00.csv")

# Lägg till en 0 framför varje telefonnummer
df["Phone"] = "0" + df["Phone"].astype(str).str.strip()

# Spara till ny fil
df.to_csv("adresser_med_gatunummer_00_phonefixed.csv", index=False)

"""

df = pd.read_csv("sebank_customers_finalfinally.csv")

# Lägg till en 0 framför varje telefonnummer
df["Phone"] = "0" + df["Phone"].astype(str).str.strip()

# Spara till ny fil
df.to_csv("sebank_customer_FINAL.csv", index=False)
