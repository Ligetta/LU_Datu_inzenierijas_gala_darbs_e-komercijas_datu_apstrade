import psycopg
import pandas as pd
import logging

def extract_data(conn_params):
    """Iegūst datus no PostgreSQL datubāzes, apvienojot tabulas. P.S. Pievienoju indeksaciju lai nerastos duplikatu kollonas parcik dataframe neliek nosaukumus ar indeksiem"""
    try:
        with psycopg.connect(**conn_params) as conn:
            query = """
                SELECT 
                    o.order_id AS order_id,
                    o.customer_id AS customer_id,
                    o.order_date,
                    o.order_time,

                    oi.order_id AS oi_order_id,
                    oi.product_id,
                    oi.quantity,
                    oi.discount,

                    p.product_id,
                    p.product_name,
                    p.category,
                    p.unit_price,

                    c.customer_id AS c_customer_id,
                    c.first_name,
                    c.last_name,
                    c.city,
                    c.state
                FROM dataeng.orders o
                JOIN dataeng.orderitems oi ON o.order_id = oi.order_id
                JOIN dataeng.products p ON oi.product_id = p.product_id
                JOIN dataeng.customers c ON o.customer_id = c.customer_id;
            """
            df = pd.read_sql(query, conn)
            logging.info("Dati veiksmīgi iegūti.")
            return df
    except Exception as e:
        logging.error(f"Kļūda datu iegūšanā: {e}")
        return pd.DataFrame()
