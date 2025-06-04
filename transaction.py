import pandas as pd

# Globala växelkurser till SEK
conversion_rates = {
    "DKK": 1.5527, "EUR": 11.6084, "GBP": 13.7361, "JPY": 0.0660, "NOK": 0.9830,
    "RMB": 1.4368, "USD": 10.2930, "ZAR": 0.5792, "ZMW": 0.3584, "AUD": 6.9731,
    "BGN": 5.8452, "BRL": 1.9697, "CAD": 7.7143, "CHF": 12.0045, "CNY": 1.4680,
    "CZK": 0.4550, "HKD": 1.3536, "HUF": 0.0289, "IDR": 0.0007, "ILS": 2.8540,
    "INR": 0.1262, "ISK": 0.0766, "KRW": 0.0078, "MXN": 0.5796, "MYR": 2.3127,
    "NZD": 6.3946, "PHP": 0.1844, "PLN": 2.6551, "RON": 2.2981, "SGD": 7.9076,
    "THB": 0.2997, "TRY": 0.3220
}


def convert_to_sek(row):
    amount = row["amount"]
    currency = row["currency"]

    if currency == "SEK":
        return amount
    elif currency in conversion_rates:
        return amount * conversion_rates[currency]
    else:
        raise ValueError(f"Unsupported currency: {currency}")


if __name__ == "__main__":
    # Testkörning
    df_customers = pd.read_csv("data/customers_clean.csv")
    print(df_customers.head())
