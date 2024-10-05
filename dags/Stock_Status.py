import Lib
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
# from airflow.hooks.base_hook import BaseHook
import psycopg2
from airflow.models import Variable


def extract_and_save_to_excel(**kwargs):
    try:

        # The SQL query to extract specific columns
        f_query_1 = """
        SELECT Article, Designation_1,Identifiant_2,  Quantite, Lot, Imputation, Date_creation, DLU, Lot_fourn, Piece
        FROM mv_db
        WHERE Designation_1 LIKE 'C/%'
        OR Designation_1 LIKE 'Short pipe%'
        AND Type_mouvement != 'Livraison client';
        """

        # The SQL query to extract specific columns
        f_query_2 = """
        SELECT Identifiant_2, Imputation, Piece, Lot, Lot_fourn,  Quantite
        FROM mv_db
        WHERE Designation_1 LIKE 'C/%'
        OR Designation_1 LIKE 'Short pipe%'
        AND Type_mouvement = 'Livraison client';
        """

        # Establish a connection to the PostgreSQL database
        hook = PostgresHook(postgres_conn_id='postgres_LH')
        connection = hook.get_conn()
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(f_query_1)
        data = cursor.fetchall()
        # print(data)

        # Get column names
        columns = [desc[0] for desc in cursor.description]

        # Convert the data into a pandas DataFrame
        df = pd.DataFrame(data, columns=columns)
        print(df.columns)

        # tr

        df.insert(df.columns.get_loc('designation_1') + 1, 'type', df['designation_1'].apply(Lib.get_first_letters))
        logging.info("New column 'First_Letter_Second_Word' added successfully")

        df.insert(df.columns.get_loc('type') + 1, 'Type de produit', df['designation_1'].apply(Lib.Type_prod))
        logging.info("New column 'Type de produit' added successfully")

        df.insert(df.columns.get_loc('Type de produit') + 1, 'Type de resine', df['designation_1'].apply(Lib.Type_resine))
        logging.info("New column 'Type de resine' added successfully")

        df.insert(df.columns.get_loc('Type de resine') + 1, 'DN', df['designation_1'].apply(Lib.DN))
        logging.info("New column 'DN' added successfully")

        df.insert(df.columns.get_loc('DN') + 1, 'PN', df['designation_1'].apply(Lib.PN))
        logging.info("New column 'PN' added successfully")

        df.insert(df.columns.get_loc('PN') + 1, 'SN', df['designation_1'].apply(Lib.SN))
        logging.info("New column 'SN' added successfully")

        # tr
        # print(df.columns)

        # Close the cursor and connection
        cursor.close()
        connection.close()

        # Define the path for the Excel file (this path could also be passed via an Airflow Variable or parameter)
        file_path = Variable.get("output_excel_file_path", default_var="/opt/airflow/Output_File/output_1.xlsx")

        # Save the DataFrame to an Excel file
        df.to_excel(file_path, index=False)

        logging.info(f"Data successfully extracted and saved to {df}")

    except Exception as e:
        logging.error(f"Error during data extraction and writing to Excel: {e}")


def extract(**kwargs):
    try:
        # File path
        file_path = '/opt/airflow/data/Mouvements_par_date.xlsx'
        df = pd.read_excel(file_path)
        #print(df)

        # Convert DataFrame to JSON-serializable format
        df = df.astype(str)

        # Push to XCom
        kwargs['ti'].xcom_push(key='extracted_data', value=df.to_dict('records'))
        print("Done")
    except Exception as e:
        print("Error during extraction:", e)


def load_to_postgres(**kwargs):
    try:
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='extracted_data', task_ids='extract_task')

        if not extracted_data:
            logging.error("No data extracted to load into PostgreSQL.")
            return

        hook = PostgresHook(postgres_conn_id='postgres_LH')
        connection = hook.get_conn()
        cursor = connection.cursor()

        # Define the insert query with placeholders for all columns
        insert_query = """
            INSERT INTO mv_db (
                Site, Depot, Article, Designation_1, Imputation, Lot, Slot, Version_majeure, Version_mineure, 
                Serie_debut, Serie_fin, Quantite, UC, Quantite_US, US, Statut, Emplacement, Identifiant_1, 
                Identifiant_2, Dem_analyse, Type_mouvement, Designation_mvt, Affaire, Tiers, Type_piece, 
                Piece, No_ligne, Type_piece_origine, Piece_origine, No_ligne_piece_orig, Lot_fourn, 
                Peremption, DLU, Recontrole, Titre, Quantite_active, UC_origine, Coeff_origine, Devise, 
                Montant_ordre, Montant_mouvement, Prix_valorise, Variation_ordre, Nature_prix_origine, 
                Variation_mouvement, Ecart_non_absorbe, Prix_ordre, Affaires, Budget, Produits, 
                Famille_mouvement_stock, Piece_comptable, Type_de_piece, Proprietaire, Inter_societes, 
                Operateur_creation, Date_creation, Heure
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        # Convert the extracted data to the list of tuples
        data_tuples = [tuple(row.values()) for row in extracted_data]

        # Execute the insert query with data
        cursor.executemany(insert_query, data_tuples)

        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error during data load: {e}")


def log_query_results():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_LH')
        sql = "SELECT * FROM mv_db;"
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            logging.info(row)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error during query log results: {e}")

def Create_table():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_LH')
        sql = """
                CREATE TABLE MV_DB (
                ID BIGSERIAL PRIMARY KEY,
                Site VARCHAR(255),
                Depot VARCHAR(255),
                Article VARCHAR(255),
                Designation_1 VARCHAR(255),
                Imputation VARCHAR(255),
                Lot VARCHAR(255),
                Slot VARCHAR(255),
                Version_majeure VARCHAR(255),
                Version_mineure VARCHAR(255),
                Serie_debut VARCHAR(255),
                Serie_fin VARCHAR(255),
                Quantite VARCHAR(255),
                UC VARCHAR(255),
                Quantite_US VARCHAR(255),
                US VARCHAR(255),
                Statut VARCHAR(255),
                Emplacement VARCHAR(255),
                Identifiant_1 VARCHAR(255),
                Identifiant_2 VARCHAR(255),
                Dem_analyse VARCHAR(255),
                Type_mouvement VARCHAR(255),
                Designation_mvt VARCHAR(255),
                Affaire VARCHAR(255),
                Tiers VARCHAR(255),
                Type_piece VARCHAR(255),
                Piece VARCHAR(255),
                No_ligne VARCHAR(255),
                Type_piece_origine VARCHAR(255),
                Piece_origine VARCHAR(255),
                No_ligne_piece_orig VARCHAR(255),
                Lot_fourn VARCHAR(255),
                Peremption VARCHAR(255),
                DLU VARCHAR(255),
                Recontrole VARCHAR(255),
                Titre VARCHAR(255),
                Quantite_active VARCHAR(255),
                UC_origine VARCHAR(255),
                Coeff_origine VARCHAR(255),
                Devise VARCHAR(255),
                Montant_ordre VARCHAR(255),
                Montant_mouvement VARCHAR(255),
                Prix_valorise VARCHAR(255),
                Variation_ordre VARCHAR(255),
                Nature_prix_origine VARCHAR(255),
                Variation_mouvement VARCHAR(255),
                Ecart_non_absorbe VARCHAR(255),
                Prix_ordre VARCHAR(255),
                Affaires VARCHAR(255),
                Budget VARCHAR(255),
                Produits VARCHAR(255),
                Famille_mouvement_stock VARCHAR(255),
                Piece_comptable VARCHAR(255),
                Type_de_piece VARCHAR(255),
                Proprietaire VARCHAR(255),
                Inter_societes VARCHAR(255),
                Operateur_creation VARCHAR(255),
                Date_creation VARCHAR(255),
                Heure VARCHAR(255)
            );
        """
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        connection.close()
        print("TABLE CREATED successfully")
    except Exception as e:
        print("Error during creation:", e)

def DELET_table():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_LH')
        sql = "DROP TABLE MV_DB;"
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        connection.close()
        print("TABLE DELETED successfully")
    except Exception as e:
        print("Error during deletion:", e)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'ETL_ex_dag',
        default_args=default_args,
        description='An ETL DAG for Excel files',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 7, 11),
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True,
    )

    delete_table_task = PythonOperator(
        task_id='delete_table_task',
        python_callable=DELET_table,
        provide_context=True,
    )

    create_table_task = PythonOperator(
        task_id='create_table_task',
        python_callable=Create_table,
        provide_context=True,
    )

    load_table_task = PythonOperator(
        task_id='load_table_task',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    log_table_results = PythonOperator(
        task_id='log_query_results',
        python_callable=log_query_results,
    )

    extract_and_save_to_excel = PythonOperator(
        task_id='extract_and_save_to_excel',
        python_callable=extract_and_save_to_excel,
        provide_context=True,
    )




    extract_task >> delete_table_task >> create_table_task >> load_table_task >> log_table_results >> extract_and_save_to_excel
