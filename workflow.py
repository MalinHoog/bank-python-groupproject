from prefect import flow, task
import pandas as pd
import time
import psycopg2

# Inställning
CSV_PATH = "data/clean_transactions.csv"

# Läs in CSV
@task
def read_data():
    df = pd.read_csv(CSV_PATH)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# Validering
@task
def validate_data(df):
    if df["amount"].isna().sum() > 0:
        raise ValueError("Amount is missing!")
    if df["currency"].isna().sum() > 0:
        raise ValueError("Currency is missing!")
    print("Validation approved.")
    return df

# Valutakonvertering med förenklat exempel
conversion_rates = {
    "EUR": 11.6, "USD": 10.3, "SEK": 1.0, "GBP": 13.7
}

@task
def convert_to_sek(df):
    df["amount_sek"] = df.apply(
        lambda row: round(row["amount"] * conversion_rates.get(str(row["currency"]).upper(), 0), 2),
        axis=1
    )
    print("Conversion to SEK completed.")
    return df

# Riskklassificering
@task
def classify_risk(df):
    high_risk = {"ZAR", "ZMW", "TRY", "BRL"}
    mod_risk = {"USD", "EUR", "GBP"}
    low_risk = {"SEK", "JPY"}

    def risk(val):
        val = str(val).upper()
        if val in high_risk:
            return "High"
        elif val in mod_risk:
            return "Moderate"
        elif val in low_risk:
            return "Low"
        return "Unknown"

    df["currency_risk"] = df["currency"].apply(risk)
    print("Risk classification completed.")
    return df

# Test
@task
def run_tests(df):
    assert df["amount_sek"].min() >= 0, "Negative SEK-amounts found!"
    print("Tests approved.")
    return True

# Export fejk databas
@task
def export_to_db(df):
    print("Exporting to database (simulated database)")
    print(f"Total {len(df)} rows ready to save.")

# Workflow
@flow
def transaction_pipeline():
    print("Starting automated workflow.")

    df = read_data()
    df = validate_data(df)
    df = convert_to_sek(df)
    df = classify_risk(df)
    run_tests(df)
    export_to_db(df)

    print("Workflow finished.")

# Kör flödet
if __name__ == "__main__":
    transaction_pipeline()
