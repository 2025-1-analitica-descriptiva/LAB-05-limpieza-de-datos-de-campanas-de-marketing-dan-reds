"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import pandas as pd
    import zipfile

    # Definimos las columnas que debe contener cada dataframe
    cliente_cols = ['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']
    campaign_cols = ['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts',
                    'previous_outcome', 'campaign_outcome']
    economics_cols = ['client_id', 'cons_price_idx', 'euribor_three_months']

    # Acumuladores para cada tipo de tabla
    clientes_totales = []
    campaigns_totales = []
    economics_totales = []

    for i in range(10):
        ruta_zip = f'./files/input/bank-marketing-campaing-{i}.csv.zip'
        
        with zipfile.ZipFile(ruta_zip, 'r') as z:
            with z.open(f'bank_marketing_{i}.csv') as f:
                df = pd.read_csv(f)

                # --- cliente.csv ---
                cliente = df[cliente_cols].copy()
                cliente['job'] = cliente['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
                cliente['education'] = cliente['education'].str.replace('.', '_', regex=False)
                cliente['education'] = cliente['education'].replace("unknown", pd.NA)
                cliente['credit_default'] = cliente['credit_default'].map(lambda x: 1 if x == 'yes' else 0)
                cliente['mortgage'] = cliente['mortgage'].map(lambda x: 1 if x == 'yes' else 0)
                clientes_totales.append(cliente)

                # --- campaign.csv ---
                campaign = df[campaign_cols].copy()
                campaign['previous_outcome'] = campaign['previous_outcome'].map(lambda x: 1 if x == 'success' else 0)
                campaign['campaign_outcome'] = campaign['campaign_outcome'].map(lambda x: 1 if x == 'yes' else 0)
                df['month'] = df['month'].str.capitalize()
                df['day'] = df['day'].astype(str)

                # Crear fecha en formato YYYY-MM-DD
                campaign['last_contact_date'] = pd.to_datetime(
                    '2022-' + df['month'] + '-' + df['day'],
                    format='%Y-%b-%d',
                    errors='coerce'
                ).dt.strftime('%Y-%m-%d')

                campaigns_totales.append(campaign)

                # --- economics.csv ---
                economics = df[economics_cols].copy()
                economics_totales.append(economics)

    # --- Exportar archivos combinados ---
    pd.concat(clientes_totales, ignore_index=True).to_csv('./files/output/client.csv', index=False, encoding='utf-8')
    pd.concat(campaigns_totales, ignore_index=True).to_csv('./files/output/campaign.csv', index=False, encoding='utf-8')
    pd.concat(economics_totales, ignore_index=True).to_csv('./files/output/economics.csv', index=False, encoding='utf-8')

    return


if __name__ == "__main__":
    clean_campaign_data()
