import pandas as pd
import re

# Läs in originalfilen
df = pd.read_csv("data/sebank_customers_with_accounts.csv")

# I den här sektionen ser vi till att telefonnummer följer samma standard, vi tar bort +46, ser till att alla börjar på 0 med en annan siffra bakom, inget börjar med två 0or, är minst 8 siffror och max 11, eller om det är ett nummer som genuit inte verkar äkta så ska det hamna i en egen fil.
def standardize_phone(phone):
    raw = str(phone).strip()
    digits = re.sub(r'\D', '', raw)

    if digits.startswith("0046"):
        digits = digits[4:]
    elif digits.startswith("46"):
        digits = digits[2:]

    digits = digits.lstrip("0")
    digits = "0" + digits

    return digits

def is_valid_phone(phone):
    digits = re.sub(r'\D', '', str(phone))
    if digits.startswith("0046"):
        digits = digits[4:]
    elif digits.startswith("46"):
        digits = digits[2:]
    digits = digits.lstrip("0")
    digits = "0" + digits
    return 8 <= len(digits) <= 11

# Skapar en ny kolumn med standardiserade nummer (utan att förstöra originalet ännu)
df["CleanedPhone"] = df["Phone"].apply(standardize_phone)
df["ValidPhone"] = df["Phone"].apply(is_valid_phone)

# I personnummer vill vi att det ska även här följa samma mönster: YYMMDD-XXXX
def clean_personnummer(pnr):
    pnr = str(pnr).strip().replace("-", "")
    return pnr[:6] + "-" + pnr[6:] if len(pnr) == 10 else pnr

df["Personnummer"] = df["Personnummer"].apply(clean_personnummer)

# Vi ser till att alla bankkonton börjar på SE8902
def clean_bank_account(acc):
    acc = str(acc).strip()
    if not acc.startswith("SE8902") and acc.startswith("8902"):
        return "SE" + acc
    return acc

df["BankAccount"] = df["BankAccount"].apply(clean_bank_account)

# Adressen delar vi upp i tre nya kolumner så det blir street, postalcode och city, samt rensar bort de som har gatunummer som börjar på 0, så 03 blir 3 osv, men att de som har 0 som gatunummer stannar kvar. Om det är någon som har två nollor i sitt gatunummer bör det rensas bort till en egen fil då vi är osäker på om det är 0 eller om det står helt fel.
streets, postcodes, cities, address_valid = [], [], [], []

for address in df["Address"]:
    try:
        street_full, location = address.split(", ")
        postcode, city = location.strip().split(" ", 1)

        match = re.match(r"(.+?)\s+(\d+)$", street_full.strip())
        if match:
            street_name, street_number = match.groups()

            if re.fullmatch(r"0{2,}", street_number):
                streets.append(None)
                postcodes.append(None)
                cities.append(None)
                address_valid.append(False)
                continue
            elif re.fullmatch(r"0+[1-9]\d*", street_number):
                street_number = str(int(street_number))

            street = f"{street_name} {street_number}"
        else:
            street = street_full

        streets.append(street)
        postcodes.append(postcode)
        cities.append(city)
        address_valid.append(True)
    except Exception:
        streets.append(None)
        postcodes.append(None)
        cities.append(None)
        address_valid.append(False)

df["Street"] = streets
df["PostalCode"] = postcodes
df["City"] = cities
df["ValidAddress"] = address_valid

# Separera rader baserat på validering
valid_rows_mask = df["ValidPhone"] & df["ValidAddress"]
clean_df = df[valid_rows_mask].copy()
garbage_df = df[~valid_rows_mask].copy()

# Använder CleanedPhone i clean_df, men behåll original i garbage_df
clean_df["Phone"] = clean_df["CleanedPhone"]

# vi rensar ut onödiga kolumner
clean_df = clean_df[["Customer", "Phone", "Personnummer", "BankAccount", "Street", "PostalCode", "City"]]
garbage_df = garbage_df[["Customer", "Phone", "Personnummer", "BankAccount", "Address"]]

# Och nu kan vi äntligen spara resultatet
clean_df.to_csv("data/customers_clean.csv", index=False)
garbage_df.to_csv("data/customers_not_valid.csv", index=False)
