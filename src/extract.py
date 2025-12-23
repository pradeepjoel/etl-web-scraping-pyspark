import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re

BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"

def extract_data():
    all_books = []

    for page in range(1, 51):  # 50 pages total
        url = BASE_URL.format(page)
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            break

        soup = BeautifulSoup(response.text, "html.parser")
        books = soup.find_all("article", class_="product_pod")

        for book in books:
            title = book.h3.a["title"]
            raw_price = book.find("p", class_="price_color").text
            price = float(re.sub(r"[^\d.]", "", raw_price))
            availability = book.find("p", class_="instock availability").text.strip()
            rating = book.p["class"][1]

            all_books.append({
                "title": title,
                "price": price,
                "availability": availability,
                "rating": rating,
                "extracted_at": datetime.utcnow()
            })

        print(f"Page {page} scraped ({len(books)} records)")

    df = pd.DataFrame(all_books)

    output_path = "data/raw/books_raw.csv"
    df.to_csv(output_path, index=False)

    print(f"\nTotal records extracted: {len(df)}")
    return output_path


if __name__ == "__main__":
    extract_data()
