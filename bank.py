# has accounts
# has customers
# can lend (from its own accounts)
# can transfer (to/from other banks)

from account import Account # Importerar Account-klassen (för att skapa och hantera konton)
from db import Db # Importerar Db-klassen (för att hantera databasanslutning)

class Bank:
    # Klassattribut som lagrar alla kunder och konton för alla instanser av Bank (bättre att flytta till __init__)
    customers = []
    accounts = []

    def __init__(self):
        # Skapar en databasanslutning via Db-klassen och sparar i instansvariabeln self.conn
        self.conn = Db().get_conn()

    def create(self, name, banknr):
        try:
            """
             Skapar en bank i databasen med namn och banknummer.
             Om banken redan finns så fångas felet och endast hämtning sker.
            """
            with self.conn:
                cursor = self.conn.cursor()
                # Använder databasanslutningen som en context manager
                cursor.execute("INSERT INTO banks (name, banknr) VALUES (%s, %s)", [name, banknr])
                # Bekräftar att insättningen ska sparas (commit)
                self.conn.commit()
                print(f"Bank '{name}' created successfully. Getting data.")
        except:
            # Fångar alla fel (t.ex. om banken redan finns, kan förbättras med specifik feltyp)
            print(f"[Warning] Bank with name {name} already exists. Getting data.")
        # Hämtar och returnerar bankinformationen från databasen
        return self.get(banknr)

    def get(self, banknr):
        """
        Hämtar en bank från databasen baserat på banknummer.
        Om den hittas, sätts instansens attribut.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM banks WHERE banknr = %s", [banknr])
        bank = cursor.fetchone()  # Hämtar en enda rad (bank) från resultatet
        if(bank[0]): # Om banken finns (bank[0] = id)
            print(f"Bank loaded.")
            self.id = bank[0] # Sätter bank-id på objektet
            self.name = bank[1] # Sätter bankens namn
            self.banknr = bank[2] # Sätter banknummer
            return self
        else:
            # Om ingen bank hittas
            print(f"[Warning] Bank with banknr {banknr} not found.")
            return None

    def add_customer(self, customer):
        """
        Lägger till en kund till bankens lista över kunder.
        Skapar också ett personkonto (standardkonto) för kunden automatiskt.
        """
        self.customers.append(customer) # Lägger till kunden i listan
        self.add_account(customer, "Personal_account", customer.ssn) # add a personal account, Skapar ett personkonto baserat på kundens personnummer
        return customer

    def add_account(self, customer, type, nr):
        """
        Skapar ett konto för en kund och lägger till i bankens lista.
        Använder Account-klassen för att skapa det nya kontot.
        """
        new_account = Account().create(customer, self, type, nr) # Skapar nytt konto
        self.accounts.append(new_account) # Lägger till kontot i bankens lista
        return new_account