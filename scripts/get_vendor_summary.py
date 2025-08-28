import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"   # fixed typo "filmode"
)

def create_vendor_summary(conn):
    query = """WITH FreightSummary AS (
        SELECT
            VendorNumber, 
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases AS p
        JOIN purchase_prices AS pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Volume, pp.Price
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.Volume,
            ps.ActualPrice,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalSalesQuantity,
            ss.TotalExciseTax,
            fs.FreightCost
    FROM PurchaseSummary AS ps
    LEFT JOIN SalesSummary AS ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary AS fs
        ON  ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC"""
    
    return pd.read_sql_query(query, conn)


def clean_data(df):
    df['Volume'] = df['Volume'].astype('float64')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # use df instead of vendor_sales_summary
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df


if __name__ == '__main__':
    # open connection
    conn = sqlite3.connect('inventory.db', timeout=30)  # add timeout to avoid "database is locked"
    try:
        logging.info('Creating Vendor Summary Table....')
        summary_df = create_vendor_summary(conn)
        logging.info(summary_df.head())

        logging.info('Cleaning Data....')
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head())

        logging.info('Ingesting Data....')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Completed')
    finally:
        conn.close()   # ✅ close connection
