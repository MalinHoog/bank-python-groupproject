import psycopg2
# Importerar psycopg2, ett Python-bibliotek för att ansluta till en PostgreSQL-databas

# Singleton to reuse the same connection across instances
# Skapar en klass Db som implementerar en singleton-designpattern, vilket innebär att endast ett objekt av denna klass kan existera under programmets livstid.
class Db:
    _instance = None
# En klassvariabel som lagrar den enda instansen av Db-klassen.

# Denna metod anropas innan __init__ när en ny instans av klassen ska skapas.
    def __new__(cls):
        if cls._instance is None: # Om det inte redan finns en instans av klassen, skapa en ny.
            cls._instance = super(Db, cls).__new__(cls) # Använder basklassen (object) för att skapa en ny instans av Db.
            cls._instance.conn = cls._create_conn() # Kallar en statisk metod för att skapa en databasanslutning och tilldelar den till instansvariabeln conn.
        return cls._instance # Returnerar den befintliga eller nyss skapade instansen. Det garanterar att alltid samma objekt används.

    @staticmethod #
    def _create_conn(): # Denna metod är oberoende av klassens instans och anropas direkt av klassen.
        return psycopg2.connect(
            dbname='bank',
            user='postgres',
            password='root',
            host='localhost',
            port='5432'
        )

# Returnerar den öppna databaskopplingen så att andra objekt kan använda den för SQL-frågor utan att öppna nya anslutningar.
    def get_conn(self):
        return self.conn

# Sammanfattning av vad koden gör:
# Sparar resurser genom att återanvända en databasanslutning.
# Ger en centraliserad plats för att hantera databasuppkopplingar.
# Lämpligt för t.ex. webbtjänster eller API:er där flera objekt behöver samma databasanslutning.