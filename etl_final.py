import logging
import os
import argparse
from dotenv import load_dotenv
from extract import extract_data
from transform import transform_data
from load import load_data

# Konfigurējam žurnāla reģistrēšanu
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Ielādējam vides mainīgos no .env faila
load_dotenv()

# Definējam savienojuma parametrus no vides mainīgajiem
conn_params = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

def run_etl(table_name, conn_params):
    """
    Izpilda visu ETL procesu:
    1. Iegūst datus no PostgreSQL datubāzes.
    2. Transformē datus.
    3. Ielādē transformētos datus PostgreSQL tabulā.
    """
    logging.info("Sākam ETL procesu...")
    df_raw = extract_data(conn_params)
    if df_raw.empty:
        logging.warning("Nav datu, ko apstrādāt.")
        return
    df_transformed = transform_data(df_raw)
    load_data(df_transformed, table_name, conn_params)
    logging.info("ETL process pabeigts.")

if __name__ == "__main__":
    # Definējam komandrindas argumentus
    parser = argparse.ArgumentParser(description="Palaist ETL procesu e-komercijas datiem.")
    parser.add_argument("--table_name", type=str, default="dataeng.customer_order_summary_adina_leimane",
                        help="Mērķa tabulas nosaukums PostgreSQL datubāzē.")
    args = parser.parse_args()

    # Palaižam ETL procesu ar norādīto tabulas nosaukumu
    run_etl(args.table_name, conn_params)
