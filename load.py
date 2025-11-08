import psycopg
import logging

def load_data(df, table_name, conn_params):
    """Ielādē apkopotos datus PostgreSQL tabulā."""
    try:
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        customer_id INT PRIMARY KEY,
                        total_orders INT,
                        total_spent DECIMAL,
                        average_order_value DECIMAL,
                        most_recent_order_date DATE,
                        first_order_date DATE,
                        days_since_first_order INT
                    );
                """)
                cur.execute(f"TRUNCATE TABLE {table_name};")

                for _, row in df.iterrows():
                    cur.execute(f"""
                        INSERT INTO {table_name}
                        (customer_id, total_orders, total_spent, average_order_value,
                         most_recent_order_date, first_order_date, days_since_first_order)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, tuple(row))
                conn.commit()
                logging.info("Dati veiksmīgi ielādēti.")
    except Exception as e:
        logging.error(f"Kļūda ielādē: {e}")
