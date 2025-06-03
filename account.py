# can withdraw
# can deposit
# has interest
# has balance
# has currency
import psycopg2
import re
import random
import string

from db import Db  # Importerar databasanslutningsklassen
from transaction import Transaction # Importerar klassen för hantering av transaktioner


class Account:

    def __init__(self):
        self.conn = Db().get_conn() # Skapar en databasanslutning
        self.balance = 0 # Initierar balansvärde till 0

    @staticmethod
    def generate_nr():
        # Returnerar ett slumpmässigt 10-siffrigt kontonummer (som sträng)
        return str(random.randint(10 ** 9, 10 ** 10 - 1))

    def create(self, name, ssn, phone, address, post_code, municipality):
        # Valideringar med regex
        if not re.fullmatch(r"\d{6}-\d{4}", ssn):
            raise ValueError("❌ Personnummer måste vara i formatet YYMMDD-XXXX")
        if not re.fullmatch(r"^0(?!0)\d+$", phone):
            raise ValueError("❌ Telefonnummer måste börja med 0 men inte 00")
        if not re.fullmatch(r"\d{5}", post_code):
            raise ValueError("❌ Postnummer måste vara exakt 5 siffror")

        # Slumpmässigt generera bankkonto enligt SE8902-XXXXNNNNNNNNNNNNNN
        cursor = self.conn.cursor()
        while True:
            letters = ''.join(random.choices(string.ascii_uppercase, k=4))
            numbers = ''.join(random.choices(string.digits, k=14))
            bank_account = f"SE8902-{letters}{numbers}"
            cursor.execute("SELECT 1 FROM bank_account WHERE account_number = %s", (bank_account,))
            if not cursor.fetchone():
                break

        try:
            with self.conn: # Databashantering med context manager
                cursor = self.conn.cursor()
                # Lägger till nytt konto i 'accounts'-tabellen
                cursor.execute("INSERT INTO accounts (customer, bank, type, nr, credit) VALUES (%s, %s, %s, %s, %s)", [customer, bank, type, nr, credit])
                self.conn.commit()
                print(f"Account '{nr}' created successfully. Getting data.")
        except:
            # Om kontot redan finns eller annan felhantering
            print(f"[Warning] Account with number {nr} already exists. Getting data.")
        return self.get(nr) # Hämtar det nyss skapade kontot från databasen

    def get(self, nr):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM accounts WHERE nr = %s", [nr])
        account = cursor.fetchone()
        if(account[0]):
            print(f"Customer loaded.")
            # Sparar kontots data till instansvariabler
            self.id = account[0]
            self.customer = account[1]
            self.bank = account[2]
            self.type = account[3]
            self.nr = account[4]
            self.credit = account[5]
            self.transactions = self.get_transactions() # Laddar transaktioner
            return self
        else:
            print(f"[Warning] Account {nr} not found.")
            return None

    def get_transactions(self):
        cursor = self.conn.cursor()
        # Hämtar alla transaktioner som hör till kontot (via kontonummer)
        cursor.execute("SELECT * FROM transactions WHERE account_nr = %s", [self.nr,])
        transactions = cursor.fetchall()
        ts = []
        for transaction in transactions:
            # Bygger upp en lista av transaktions-dictionaries
            ts.append({
                "id": transaction[0], # Transaktions-ID
                "amount": transaction[1],  # Belopp (positivt eller negativt)
                "account": transaction[2] # Kontonummer
            })
        return ts

    def get_balance(self):
        balance = 0
        # Summerar alla transaktioners belopp
        for transaction in self.get_transactions():
            balance += transaction['amount']
        self.balance = balance # Sätter och returnerar balansen
        return balance

    def deposit(self, amount):
        if amount > 0:
            # Skapar en transaktion med positivt belopp (insättning)
            Transaction().create(amount, self)

    def withdraw(self, amount):
        # Kontrollera om tillgängligt saldo + kredit räcker för uttaget
        if(amount <= self.get_balance() + self.credit):
            # Skapar en transaktion med negativt belopp (uttag)
            Transaction().create(-amount, self)
            return -amount # Returnerar uttaget belopp
        else:
            return 0 # Uttag nekas, för lite pengar

