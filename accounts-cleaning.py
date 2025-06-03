import pandas as pd
import re

# Läs in originalfilen
df = pd.read_csv("data/sebank_customers_with_accounts.csv")


# Först rensar vi bland telefonnummer så alla följer samma mönster, alltså så alla börjar på 0 och inte +46 (0) eller liknande, inga mellanslag eller bindesträck
def clean_phone(phone):
    phone = str(phone).strip()
    phone = re.sub(r'\D', '', phone)  # Ta bort allt som inte är siffror
    if phone.startswith("46"):
        phone = "0" + phone[2:]
    elif phone.startswith("0046"):
        phone = "0" + phone[4:]
    elif not phone.startswith("0"):
        phone = "0" + phone
    return phone


df["Phone"] = df["Phone"].apply(clean_phone)


# ser till att all data i personnummer-kolummnen följer samma mönster (YYMMDD-XXXX)
def clean_personnummer(pnr):
    pnr = str(pnr).strip().replace("-", "")
    return pnr[:6] + "-" + pnr[6:] if len(pnr) == 10 else pnr


df["Personnummer"] = df["Personnummer"].apply(clean_personnummer)


# dubbelkollar att alla accounts startar med SE8902, och om de istället skulle råka starta med 8902 så konverteras de om till att följa rätt mönster
def clean_bank_account(acc):
    acc = str(acc).strip()
    if not acc.startswith("SE8902"):
        if acc.startswith("8902"):
            return "SE" + acc
    return acc


df["BankAccount"] = df["BankAccount"].apply(clean_bank_account)

# Address var lite mer komplicerad för vi ville inte att de skulle ligga i en egen kolumn, så den delas upp i street, postcode och city, droppar den forna address kolumnen och mer: alla gatunummer som börjar på 0 eller 00 osv redigeras till att endast vara nästkommande siffra, om gatunummret är 0 lämnas det, men om det skulle vara två nollor eller flera så ska de hamna i en egen fil för att skickas tillbaka till banken - ska det vara 0 eller är det helt fel siffror?
streets, postcodes, cities, garbage_rows = [], [], [], []

for address in df["Address"]:
    try:
        street_full, location = address.split(", ")
        postcode, city = location.strip().split(" ", 1)

        # Ta ut gatunamn och nummer
        match = re.match(r"(.+?)\s+(\d+)$", street_full.strip())
        if match:
            street_name, street_number = match.groups()

            # Specialhantering av gatunummer som är "00", "003" etc.
            if re.fullmatch(r"0{2,}", street_number):
                garbage_rows.append((street_full, postcode, city))
                streets.append(None)
                postcodes.append(None)
                cities.append(None)
                continue
            elif re.fullmatch(r"0+[1-9]\d*", street_number):  # t.ex. 03 → 3
                street_number = str(int(street_number))

            street = f"{street_name} {street_number}"
        else:
            # Om inget matchar, lägg hela
            street = street_full

        streets.append(street)
        postcodes.append(postcode)
        cities.append(city)
    except Exception:
        streets.append(None)
        postcodes.append(None)
        cities.append(None)

df["Street"] = streets
df["PostalCode"] = postcodes
df["City"] = cities

clean_df = df.dropna(subset=["Street", "PostalCode", "City"])
garbage_df = df[df["Street"].isna()]

# Vi lägger om så att kolumnerna hamnar i en specfik ordning
clean_df = clean_df[["Customer", "Phone", "Personnummer", "BankAccount", "Street", "PostalCode", "City"]]
garbage_df = garbage_df[["Customer", "Phone", "Personnummer", "BankAccount", "Address"]]

# tillslut kan vi spara resultatet i två olika filer
clean_df.to_csv("data/sebank_customers_cleanedandready.csv", index=False)
garbage_df.to_csv("data/sebank_customers_address_00_only.csv", index=False)
