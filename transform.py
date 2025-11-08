import pandas as pd
import logging

def transform_data(df):
    """Transformē iegūtos datus no datubāzes."""

    try:
        #Pārbaudam, vai ir trūkstošas, ja ir, aizvieto.

        # Pieņemam, ka 'discount' var būt NULL, ja nav atlaides — aizvietojam ar 0.0
        # 'quantity' var būt NULL, ja kļūda ievadē — aizvietojam ar 1 (minimālais loģiskais daudzums)
        # 'unit_price' var būt NULL, ja produkts nav pareizi reģistrēts — aizvietojam ar 0.0, lai neradītu kļūdas aprēķinos
        df.fillna({
            'discount': 0.0,
            'quantity': 1,
            'unit_price': 0.0
        }, inplace=True)

        # Datu tipu pārbaude un konvertēšana (ja nepieciešams)

        # Pārliecināmies, ka skaitliskās kolonnas ir pareizā formātā
        df['discount'] = df['discount'].astype(float)
        df['quantity'] = df['quantity'].astype(int)
        df['unit_price'] = df['unit_price'].astype(float)

        # Datu transformācija

        # Apvienojam datumu un laiku vienā kolonnā
        df['order_datetime'] = pd.to_datetime(df['order_date'].astype(str) + ' ' + df['order_time'].astype(str))

        # Aprēķinām kopējo summu par pasūtījumu (ar atlaidi)
        df['total_amount'] = df['quantity'] * df['unit_price'] * (1 - df['discount'])

        # Izveidojam pilnu klienta vārdu
        df['customer_full_name'] = df['first_name'] + ' ' + df['last_name']

        # Apkopojums pēc klienta -> tas ir definets ka ir vajadzigs load punkta
        summary = df.groupby('customer_id').agg(
            total_orders=('order_id', 'nunique'),
            total_spent=('total_amount', 'sum'),
            average_order_value=('total_amount', 'mean'),
            most_recent_order_date=('order_datetime', 'max'),
            first_order_date=('order_datetime', 'min')
        ).reset_index()

        # Aprēķinām dienas kopš pirmā pasūtījuma -> tas ir definets ka ir vajadzigs load punkta
        summary['days_since_first_order'] = (pd.Timestamp.now() - summary['first_order_date']).dt.days

        logging.info("Dati veiksmīgi transformēti.")
        return summary

    except Exception as e:
        logging.error(f"Kļūda transformācijā: {e}")
        return pd.DataFrame()
