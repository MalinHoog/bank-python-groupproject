import pandas as pd
import re

# Körd kod för att rensa ut null värden i alla kolumner (det verkar inte som att det finns några nullvärden i sebank_customers_with_accounts.csv)
df = pd.read_csv("data\sebank_customers_with_accounts.csv")

# Hitta alla rader som har minst ett null-värde
rader_med_null = df[df.isnull().any(axis=1)]

# Om det finns rader med nullvärden så hamnar det i den här filen
rader_med_null.to_csv("sebank_customers_rader_med_null.csv", index=False)
# Men det fanns inget alls faktiskt, yey!

# Om det finns nullvärden så droppas de från filen och sparas
df = df.dropna()

# Sparar över den rensade datan till en ny fil
df.to_csv("sebank_customers_cleaned_no_nulls.csv", index=False)

print("Rader med null sparades i 'sebank_customers_rader_med_null.csv'")
print("Rensad data sparades i 'sebank_customers_cleaned_no_nulls.csv'")


# Körd kod som snyggar till data, bland annat delar upp adressen, ändrar telefonnummer (så att alla telefonnummer börjar med 0) personnummer (så att alla följer samma mönster med 10 siffror: YYMMDD-XXXX) och att alla banknummer börjar på SE8902. Samt droppar den tidigare kolumnen Address för att undvika dubbeldata.

# Normalisera telefonnummer till format som börjar med 0
def clean_phone(phone):
    phone = str(phone).strip()
    # Ta bort +46 eller +46 (0) och ersätt med 0
    phone = re.sub(r'^\+46\s*\(?0?\)?', '0', phone)
    return phone

df["Phone"] = df["Phone"].apply(clean_phone)

# Säkerställ att personnummer har formatet ÅÅMMDD-NNNN
def format_personnummer(pnr):
    pnr = str(pnr).strip()
    match = re.match(r'^(\d{6})[-]?(\d{4})$', pnr)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return pnr  # returnerar som det är om det inte matchar

df["Personnummer"] = df["Personnummer"].apply(format_personnummer)

# Dela upp adress i Address, PostalCode, City
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

# Säkerställ att postnummer består av exakt 5 siffror
df["PostalCode"] = df["PostalCode"].apply(lambda x: x if re.match(r'^\d{5}$', str(x)) else "")

# Säkerställ att kontonummer börjar med SE8902
def fix_account(acc):
    acc = str(acc).strip()
    if acc.startswith("8902"):
        return "SE" + acc
    elif not acc.startswith("SE"):
        return "SE8902" + acc
    return acc

df["BankAccount"] = df["BankAccount"].apply(fix_account)

# Sparar resultatet till en ny CSV-fil
df.to_csv("sebank_customers_cleaned.csv", index=False)



# Kod som körs för att snygga till telefonnummer, så att det inte finns några mellanslag eller bindesträck.

df = pd.read_csv("sebank_customers_cleaned.csv")

# Ta bort mellanslag och bindestreck i telefonnummer
df["Phone"] = df["Phone"].str.replace(" ", "").str.replace("-", "")

df.to_csv("sebank_customers_cleaned_again.csv", index=False)



# vi upptäckte att det fanns vissa adresser som heter typ storgatan 0 osv, så vi bestämde bara att ta en titt på dessa för att se om det är något som inte ser helt ok ut.
df = pd.read_csv("sebank_customers_cleaned_again.csv")

matchar_nollor = df[df["Street"].astype(str).str.contains(r"\b0{1,3}\b", regex=True)]

print(matchar_nollor[["Customer", "Street"]])
# i den sökningen kan vi se att det finns en som har en gata som är parkstigen 00, vilket vi inte tror stämmer, så den personen kommer rensas ut i en egen fil - en person vi tycker banken kanske ska ta beslutet själv om det ska redigeras eller blockas??



# i den här rensningen så rensar vi ut de adresser som verkar ha lite udda gatunummer, typ 00
df = pd.read_csv("sebank_customers_cleaned_again.csv")

# Matcha rader där Street slutar med ett mellanslag + exakt "00"
med_gatunummer_00 = df[df["Street"].astype(str).str.strip().str.match(r".*\s00$")]

# Sparar dessa till egen fil
med_gatunummer_00.to_csv("adresser_med_gatunummer_00.csv", index=False)
# denna fil bör vi skicka tillbaka till banken som en person som bör eventuellt ses över

# Skapa rensad DataFrame utan dessa rader
utan_gatunummer_00 = df[~df["Street"].astype(str).str.strip().str.match(r".*\s00$")]

# Spara rensad data
utan_gatunummer_00.to_csv("sebank_customers_superclean.csv", index=False)

# nu ska det vara rent och snyggt, så sebank_customers_superclean.csv är den senaste och den filen vi kommer använda i arbetet