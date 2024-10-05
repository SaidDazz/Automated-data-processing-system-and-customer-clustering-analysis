import Lib
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
# from airflow.hooks.base_hook import BaseHook
# import psycopg2
from airflow.models import Variable


def extract_and_save_to_excel(**kwargs):
    try:
        # Define your SQL queries for each sheet
        queries = [
            {
                "query": """
                    SELECT Identifiant_2, Imputation, Piece, Lot, Lot_fourn,  Quantite, Designation_1
                    FROM mv_db
                    WHERE Designation_1 LIKE 'C/%'
                    OR Designation_1 LIKE 'Short pipe%'
                    AND Type_mouvement = 'Livraison client'
                    AND Article NOT LIKE 'MP%'
                    AND Emplacement NOT LIKE 'MP%';
                """,
                "sheet_name": "Conduit"
            },
            {
                "query": """
                    SELECT Identifiant_2, Imputation, Piece, Lot, Lot_fourn,  Quantite, Designation_1
                    FROM mv_db
                    WHERE Designation_1 NOT LIKE 'C/%'
                    AND Designation_1 NOT LIKE 'Short pipe%'
                    AND Designation_1 NOT LIKE 'Raccord%'
                    AND Type_mouvement = 'Livraison client'
                    AND Article NOT LIKE 'MP%'
                    AND Emplacement NOT LIKE 'MP%';
                """,
                "sheet_name": "Pièces spéciales"
            },
            {
                "query": """
                    SELECT Identifiant_2, Imputation, Piece, Lot, Quantite, Designation_1
                    FROM mv_db
                    WHERE Designation_1 LIKE 'Raccord%'
                    AND Type_mouvement = 'Livraison client'
                    AND Article NOT LIKE 'MP%'
                    AND Emplacement NOT LIKE 'MP%';
                """,
                "sheet_name": "Raccords"
            },
            {
                "query": """
                    SELECT Identifiant_2, Imputation, Piece, Designation_1, Quantite, UC
                    FROM mv_db
                    WHERE Designation_1 LIKE 'Kit%'
                    AND Type_mouvement = 'Livraison client';
                """,
                "sheet_name": "Materiele"
            },
            {
                "query": """
                    SELECT Identifiant_2, Imputation, Piece, Designation_1, Quantite, UC
                    FROM mv_db
                    WHERE Designation_1 LIKE 'Joint%'
                    AND Type_mouvement = 'Livraison client';
                """,
                "sheet_name": "Joints"
            },
            {
                "query": """
                    SELECT Identifiant_2, Imputation, Piece, Designation_1, Quantite, UC
                    FROM mv_db
                    WHERE Designation_1 LIKE 'stoppers%'
                    AND Type_mouvement = 'Livraison client';
                """,
                "sheet_name": "Stoppeurs"
            }
        ]

        # Establish a connection to the PostgreSQL database
        hook = PostgresHook(postgres_conn_id='postgres_LH')
        connection = hook.get_conn()
        cursor = connection.cursor()

        # List to hold the DataFrames
        dataframes = []

        for query_info in queries:
            # Execute the query
            cursor.execute(query_info["query"])
            data = cursor.fetchall()

            # Get column names for the query
            columns = [desc[0] for desc in cursor.description]

            # Convert the data into a DataFrame
            df = pd.DataFrame(data, columns=columns)
            # print(df.columns)

            # Perform any modifications on the DataFrame
            if query_info["sheet_name"] == 'Conduit':

                df.insert(df.columns.get_loc('lot') + 1, 'Machine', df['lot'].apply(Lib.machine))
                logging.info("New column 'Machine' added successfully")

                df.insert(df.columns.get_loc('Machine') + 1, 'Jour', df['lot'].apply(Lib.annee1))
                logging.info("New column 'année' added successfully")

                df.insert(df.columns.get_loc('Jour') + 1, 'mois', df['lot'].apply(Lib.Annee2))
                logging.info("New column 'mois' added successfully")

                df.insert(df.columns.get_loc('mois') + 1, 'Année', df['lot'].apply(Lib.annee1))
                logging.info("New column 'Année' added successfully")

                df.insert(df.columns.get_loc('lot_fourn') + 1, 'DN', df['designation_1'].apply(Lib.DN))
                df.insert(df.columns.get_loc('DN') + 1, 'PN', df['designation_1'].apply(Lib.PN))
                df.insert(df.columns.get_loc('PN') + 1, 'SN', df['designation_1'].apply(Lib.SN))

            elif query_info["sheet_name"] == 'Pièces spéciales':
                df.insert(df.columns.get_loc('piece') + 1, 'Angle', df['designation_1'].apply(Lib.angle))
                logging.info("New column 'Angle' added successfully")

                df.insert(df.columns.get_loc('Angle') + 1, 'NOZEL', df['designation_1'].apply(Lib.nozel))
                logging.info("New column 'NOZEL' added successfully")

                df.insert(df.columns.get_loc('lot') + 1, 'Machine', df['lot'].apply(Lib.machine))
                logging.info("New column 'Machine' added successfully")

                df.insert(df.columns.get_loc('Machine') + 1, 'Date Production', df['lot'].apply(Lib.annee1))
                logging.info("New column 'Date Production' added successfully")

                df.insert(df.columns.get_loc('Date Production') + 1, 'Année Production', df['lot'].apply(Lib.annee_p1))
                logging.info("New column 'Année Production' added successfully")

                df.insert(df.columns.get_loc('quantite') + 1, 'Type', df['designation_1'].apply(Lib.type_p))
                logging.info("New column 'Type' added successfully")

                df.insert(df.columns.get_loc('Type') + 1, 'DN', df['designation_1'].apply(Lib.DN_p))
                df.insert(df.columns.get_loc('DN') + 1, 'PN', df['designation_1'].apply(Lib.PN_p))
                df.insert(df.columns.get_loc('PN') + 1, 'SN', df['designation_1'].apply(Lib.SN_p))

            elif query_info["sheet_name"] == 'Raccords':

                df.insert(df.columns.get_loc('piece') + 1, 'DN', df['designation_1'].apply(Lib.DN_r))
                df.insert(df.columns.get_loc('DN') + 1, 'PN', df['designation_1'].apply(Lib.PN_r))

            elif query_info["sheet_name"] == 'Joints':
                df.insert(df.columns.get_loc('designation_1') + 1, 'DN', df['designation_1'].apply(Lib.DN_j))

            elif query_info["sheet_name"] == 'Stoppeurs':
                df.insert(df.columns.get_loc('designation_1') + 1, 'DN', df['designation_1'].apply(Lib.DN))
            else:
                print("****************")

            # Add the DataFrame to the list along with its sheet name
            dataframes.append((df, query_info["sheet_name"]))

        # Close the cursor and connection
        cursor.close()
        connection.close()

        # Define the path for the Excel file
        file_path = Variable.get("output_excel_file_path", default_var="/opt/airflow/Output_File/Livraison_2024.xlsx")

        # Use ExcelWriter to save the DataFrames to different sheets
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            for df, sheet_name in dataframes:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        logging.info(f"Data successfully extracted, modified, and saved to {file_path}")

    except Exception as e:
        logging.error(f"Error during data extraction, modification, or writing to Excel: {e}")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'ETL_ex_dag_Liv',
        default_args=default_args,
        description='An ETL DAG for Excel files',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 7, 11),
        catchup=False,
) as dag:

    extract_and_save_to_excel = PythonOperator(
        task_id='extract_and_save_to_excel',
        python_callable=extract_and_save_to_excel,
        provide_context=True,
    )

    extract_and_save_to_excel