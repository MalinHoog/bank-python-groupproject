import pandas as pd

# körd kod för att rensa ut null värden i alla kolumner (det verkar inte som att det finns några nullvärden i sebank_customers_with_accounts.csv)
""" 
df = pd.read_csv("data\sebank_customers_with_accounts.csv")

# 1. Hitta alla rader som har minst ett null-värde
rader_med_null = df[df.isnull().any(axis=1)]

# 2. Spara dessa rader i en separat fil
rader_med_null.to_csv("sebank_customers_rader_med_null.csv", index=False)

# 3. Ta bort raderna från original-DataFrame
df = df.dropna()

# 4. (Valfritt) Spara den rensade DataFrame till en ny fil eller skriva över
df.to_csv("sebank_customers_cleaned_no_nulls.csv", index=False)

print("Rader med null sparades i 'sebank_customers_rader_med_null.csv'")
print("Rensad data sparades i 'sebank_customers_cleaned_no_nulls.csv'")
"""


# Körd kod som snyggar till data, bland annat delar upp adressen, ändrar telefonnummer (så att alla telefonnummer börjar med 0) personnummer (så att alla följer samma mönster med 10 siffror: YYMMDD-XXXX) och att alla banknummer börjar på SE8902. Samt droppar den tidigare kolumnen Address för att undvika dubbeldata.
"""
# 1. Normalisera telefonnummer till format som börjar med 0
def clean_phone(phone):
    phone = str(phone).strip()
    # Ta bort +46 eller +46 (0) och ersätt med 0
    phone = re.sub(r'^\+46\s*\(?0?\)?', '0', phone)
    return phone

df["Phone"] = df["Phone"].apply(clean_phone)

# 2. Säkerställ att personnummer har formatet ÅÅMMDD-NNNN
def format_personnummer(pnr):
    pnr = str(pnr).strip()
    match = re.match(r'^(\d{6})[-]?(\d{4})$', pnr)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return pnr  # returnerar som det är om det inte matchar

df["Personnummer"] = df["Personnummer"].apply(format_personnummer)

# 3. Dela upp adress i Address, PostalCode, City
def split_address(full_address):
    try:
        street, rest = full_address.rsplit(",", 1)
        rest_parts = rest.strip().split(" ", 1)
        postal_code = rest_parts[0]
        city = rest_parts[1] if len(rest_parts) > 1 else ""
        return pd.Series([street.strip(), postal_code.strip(), city.strip()])
    except Exception:
        return pd.Series([full_address, "", ""])

df[["Street", "PostalCode", "City"]] = df["Address"].apply(split_address)
df.drop(columns=["Address"], inplace=True)

# 4. Säkerställ att postnummer består av exakt 5 siffror
df["PostalCode"] = df["PostalCode"].apply(lambda x: x if re.match(r'^\d{5}$', str(x)) else "")

# 5. Säkerställ att kontonummer börjar med SE8902
def fix_account(acc):
    acc = str(acc).strip()
    if acc.startswith("8902"):
        return "SE" + acc
    elif not acc.startswith("SE"):
        return "SE8902" + acc
    return acc

df["BankAccount"] = df["BankAccount"].apply(fix_account)


# 6. Spara resultatet till en ny CSV-fil
df.to_csv("sebank_customers_cleaned.csv", index=False)

# print("Filen har rengjorts och sparats som 'sebank_customers_cleaned.csv'")
"""

