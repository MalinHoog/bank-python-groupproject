
# Datavalidering & transaktionsanalys

## Syfte
Målet med detta projekt är att utveckla ett fungerande och skalbart ETL-flöde för en bankapplikation som analyserar och validerar stora mängder transaktionsdata. Fokus ligger på att säkerställa datakvalitet. Vi ville även identifiera avvikelser (såsom penningtvätt, smurfing, self-transfers) och början på att skapa ett spårbart, automatiserat arbetsflöde för datavalidering och import.

## Projektstruktur
```
bank-python-groupproject/
│
├── alembic/                           	# Alembic: versionsstyrning av databasschema
│   ├── versions/                      	# Versionsmappar från Alembic-migrering
│   │   └── <timestamp>_new_database.py	# Migration: skapar tabeller
│   ├── env.py                         	# Alembic-konfiguration
│   └── script.py.mako                 	# Mallfil för Alembic-migrationer
│
├── data/                              	# Input/Output-data i CSV-format
│   ├── transactions.csv               	# Rå transaktionsdata
│   ├── clean_transactions.csv         	# Rensade & konverterade transaktioner
│   ├── customers_clean.csv            	# Rensade kunder
│   ├── sebank_customers_with_accounts.csv # Rå kunddata med konton
│   ├── flagged_self_transfers.csv     	# Flaggade transaktioner
│   ├── flagged_frequent_daily_senders.csv # Flaggade transaktioner
│   ├── flagged_smurfing.csv           	# Flaggade transaktioner
│   ├── flagged_high_risk.csv          	# Flaggade transaktioner
│   ├── flagged_international.csv      	# Flaggade transaktioner
│   └── ...
│
├── tests/
│   └── test_transactions.py           	# Enhetstester med Pytest
│
├── .gitignore                         	# Uteslut oönskade filer från Git
├── alembic.ini                        	# Alembic-konfiguration
├── create_db.sql                      	# SQL: manuellt skapa databas
├── requirements.txt                   	# Alla beroenden (pandas, GX, psycopg2 etc.)
├── README.md                          	# Dokumentation av projektet
│
├── app.py                             	# Eventuell entrypoint
├── account.py                         	# Konto-logik (saldo, uttag, transaktioner)
├── accounts_cleaning.py               	# Validering & rensning av kontodata
├── bank.py                            	# Bank-logik (skapa bank, kunder, konton)
├── bank_boss_womans.py                 # Kör import till databas (med rollback)
├── customer.py                        	# Kundklass: skapa, hämta, koppla konton
├── db.py                              	# Singleton för databasanslutning
├── models.py                          	# SQLAlchemy-modeller för databasen
├── transaction.py                     	# Logik för att skapa transaktioner
├── dataCleaning.py                    	# Rensning och validering av transaktionsdata
├── workflow.py                        	# Prefect-workflow för körning av processer
│
├── customers_logs.ipynb               	# Loggning och analys av kunddata
├── customers_logs.txt                 	# Loggfil från körning av ovan
├── customers-report.ipynb             	# Rapportfil: analyserar kunddata
│
├── transaction_report.ipynb           	# Rapportfil: analyserar transaktioner
├── transactions_logs.ipynb            	# Loggning & flaggning av transaktionsdata
├── transactions_logs.txt              	# Loggfil från ovan
│
├── validateTransactionsFinal.ipynb    	# Validerar transaktionsdata (GX)
└── validatingAccountsFinal.ipynb      	# Validerar kontodata (GX)
```

## Hur man kör projektet
### 1. Klona repot
```bash
git clone https://github.com/MalinHoog/bank-python-groupproject.git
cd bank-python-groupproject
```

### 2. Installera beroenden
```bash
pip install -r requirements.txt
```

### 3. Skapa och initiera databasen
```bash
psql -U postgres
\i create_db.sql
```
Alternativt:
```bash
alembic upgrade head
```

### 4. Starta dataladdning (import till DB)
```bash
python bank_boss_womans.py
```

## Testning
Vi har använt `pytest` för att skapa enhetstester (finns i `tests/`) samt manuella kontroller i Jupyter.

## Datavalidering
### Med Great Expectations:
- Validerar t.ex. att valutor följer format (`^[A-Z]{3}$`).
- Transaktions-ID matchar UUID-format.
- Belopp inom rimligt intervall.
- Inga NULL-värden på kritiska fält.

### Med Pandas / Regex:
- Städat upp telefonnummer, postnummer, personnummer, gatuadresser med gatunummer.
- Säkerställt format som "YYYYMMDD-XXXX", "SE8902..."
- Säkerställer att currency har formatet SEK, USD (tre stora bokstäver), “ r"^[A-Z]{3}$".
- Säkerställer att bankAccount har formatet “r"^SE8902".
- Med df så har vi kunnat lägga till en fix för telefonnummer så att det alltid börjar med 0 eftersom excel tar bort en nolla. 

## Workflow & ETL
Vi har byggt ett automatiserat arbetsflöde med Prefect:
- Rensa inkommande datafiler.
- Validerar data (GX + Pandas).
- Skapar loggar (Jupyter + .txt).
- Sparar resultat i CSV.
- Laddar in till PostgreSQL-databas.
- Prefect-flow och @task-dekorationer.
- Stegvis bearbetning av transaktionsdata.
- Riskklassificering och valutakonvertering.
- Test på negativa belopp.
- Automatisk körning via if__name__==”main”.

## Databas & migrering
- PostgreSQL som huvuddatabas.
- Alembic används för schemahantering.
- SQLAlchemy ORM i Python.
- Constraints: PRIMARY KEY, UNIQUE, DEFAULT.

Rollback används vid fel i import (se try/except i bank_boss_womans.py).

## Rapporter
- Skapade loggfiler för flaggade transaktioner (self-transfers, high risk, smurfing).
- Exporterade som CSV för insyn/kontroll av banken.
- Visualiseringar för riskanalys, avvikelse och kundstruktur.

## Datakvalitet och ACID
Vi arbetar för att uppnå hög datakvalitet genom:

| Mål        | Beskrivning                                                                 |
|-------------|-----------------------------------------------------------------------------|
| Noggrannhet | Säkerställt via validering, t.ex. valutaformat, korrekt konvertering       |
| Fullständighet | NULL-kontroller, rensning av inkommande data                            |
| Konsistens  | Samma regler tillämpas över alla filer och valideringslager                |
| Validitet   | Regex + GX + constraints                                                    |
| Unikhet     | UUID, unika konton, personnummer                                            |
| Aktualitet  | Workflow bygger förutsättningar för framtida realtidskörning                |
              

## Gruppmedlemmar

| Namn | GitHub |
|------|--------|
| Malin Hööglund | [@MalinHoog](https://github.com/MalinHoog) |
| Linda Hiermann | [@Lompish](https://github.com/Lompish) |
| Nadia Messary | [@nadiamessary](https://github.com/nadiamessary) |
| Evelina Nilsson | [@evenil87](https://github.com/evenil87) |


## Möjliga förbättringar
- Dela upp loggar per risktyp i olika CSV-filer.
- Bygga ett automatiserat arbetsflöde med Prefect och koppla till vår faktiska databas.
- Gruppera skript i undermappar (`src/`, `validators/`, `logs/`)
- Utöka och strukturera bättre databasen för att undvika dubbeldata.
- Lägg till triggers.
- Hitta ett sätt att realtidsuppdatera valutaomvandlare.
- Lägga till flera tester med pytest.
- Under längre tid “flagga” återkommande konton med varningar.
- I nuläget så har något ändrats i dataCleaning.py så att null-värden inte filtreras bort. Det kan man se i validateTransactionsFinal.ipynb. Detta ska uppdateras så att de filtreras bort. Även accounts_cleaning.py.


## Slutsats
Projektet demonstrerar ett nästintill komplett ETL-flöde från rådata till rensad, validerad och laddad data i databas. 
Vi har använt oss utav testning, validering, rollback-hantering, och workflow management. 
Det ska även uppfylla kraven på spårbarhet och skalbarhet för att identifiera och analysera misstänkta transaktioner i en bankmiljö. 
Vi har identifierat områden som kan och behöver förbättring men med den tiden vi har haft så är vi mycket nöjda med det vi har att presentera.

