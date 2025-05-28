from db import Db
# Importerar klassen Db från modulen db, som är den singletonklass vi analyserade tidigare. Den används för att hämta en återanvändbar databasanslutning.

class Transaction:
# Definierar en klass Transaction som hanterar skapandet av transaktioner i en databas (troligtvis i en bankapplikation).

# När ett nytt Transaction-objekt skapas:
# Hämtas en databasanslutning via Db-singletonen med Db().get_conn().
# Anslutningen sparas i self.conn, vilket innebär att varje Transaction-instans har tillgång till samma återanvändbara anslutning.
    def __init__(self):
        self.conn = Db().get_conn()

# Metoden create tar två argument:
# amount: Summan som ska sättas in eller tas ut.
# account: Ett objekt (troligen en instans av en klass Account) som förväntas ha ett attribut nr (kontonummer).
    def create(self, amount, account):
        try: # try block används för att fånga eventuella fel vid databasoperationen.
            with self.conn: # är en kontextmanager som säkerställer att transaktionen hanteras korrekt – t.ex. att commit/rollback sker automatiskt.
                cursor = self.conn.cursor() # Skapar en ny databaspekare (cursor) för att kunna köra SQL-frågor.
                cursor.execute("INSERT INTO transactions (amount, account_nr) VALUES (%s, %s)", [amount, account.nr]) #Kör en parameteriserad SQL-sats som lägger till en ny rad i tabellen transactions med kolumnerna amount och account_nr.
                # %s används som platshållare, och värdena sätts separat som en lista ([amount, account.nr]) för att skydda mot SQL-injektion.
                self.conn.commit() # Bekräftar ändringen i databasen (även om with-block ofta gör detta automatiskt, är det ändå vanligt att inkludera commit() explicitt).
                print(f"Transaction '{amount}' created successfully.") # Skriver ut ett meddelande om att transaktionen lyckades.
        except:
            print(f"[Warning] Transaction blocked due to constraint violation, date or non approved customer.")
            # Om något fel inträffar (t.ex. att SQL-satsen bryter mot en begränsning eller att kontot inte är godkänt), fångas felet här och ett varningsmeddelande skrivs ut.
        return amount # Returnerar beloppet som försöktes sättas in – oavsett om transaktionen lyckades eller inte.
