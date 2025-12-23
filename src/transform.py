import pandas as pd

def transform_data():
    df = pd.read_csv("data/raw/books_raw.csv")

    # Remove missing values
    df.dropna(inplace=True)

    # Ensure price is positive
    df = df[df["price"] > 0]

    # Standardize availability
    df["availability"] = df["availability"].str.lower()

    df.to_csv("data/processed/books_clean.csv", index=False)
    return df

if __name__ == "__main__":
    transform_data()
