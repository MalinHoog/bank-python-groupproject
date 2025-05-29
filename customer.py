# has accounts
# can apply for an account
# can borrow
# can ask for credit
# can try update personal info

# Importerar klasserna som används
from account import Account # För att hämta kundens konton
from db import Db # För att koppla till databasen


class Customer:
    accounts = [] # Klassattribut: lista över kundens konton (bättre att ha som instansattribut)

    def __init__(self): # Konstruktor som körs när ett nytt Customer-objekt skapas
        self.conn = Db().get_conn() # Hämtar en databasanslutning via Db-klassen

    def create(self, name, ssn):
        try:
            with self.conn: # Använder databasanslutningen som en context manager
                cursor = self.conn.cursor()
                # Lägger in ny kund i 'customers'-tabellen med namn och personnummer
                cursor.execute("INSERT INTO customers (name, ssn) VALUES (%s, %s)",[name, ssn])
                self.conn.commit() # Sparar ändringen i databasen
                print(f"Customer '{name}' created successfully. Getting data.")
        except:
            # Fångar fel om kunden redan finns (eller andra fel), och fortsätter med att hämta kundens data
            print(f"[Warning] Customer {name} already exists. Getting data.")
        return self.get(ssn)  # Hämtar och returnerar kundobjektet

    def get(self, ssn):
        cursor = self.conn.cursor()
        # Hämtar kundinformation baserat på personnummer
        cursor.execute("SELECT * FROM customers WHERE ssn = %s", [ssn])
        customer = cursor.fetchone() # Hämtar första (enda) raden
        if(customer[0]): # Om en kund hittas
            print(f"Customer loaded.")
            self.id = customer[0] # Sätter kundens ID
            self.name = customer[1] # Sätter kundens namn
            self.ssn = customer[2] # Sätter kundens personnummer
            self.accounts = self.get_accounts() # Hämtar kundens konton från databasen
            return self # Returnerar det färdigladdade kundobjektet
        else:
            # Om kunden inte hittas
            print(f"[Warning] Customer with ssn {ssn} not found.")
            return None

    def get_accounts(self):
        cursor = self.conn.cursor()
        # Hämtar alla konton där 'customer' är kopplat till denna kunds ID
        cursor.execute("SELECT * FROM accounts WHERE customer = %s", [self.id,])
        accounts = cursor.fetchall() # Hämtar alla matchande rader
        accs = [] # Lista som ska innehålla Account-objekt
        for account in accounts:
            # Skapar Account-objekt från kontonummer (kolumn index 4 i raden)
            accs.append(Account().get(account[4]))
        return accs # Returnerar listan med kundens konton


