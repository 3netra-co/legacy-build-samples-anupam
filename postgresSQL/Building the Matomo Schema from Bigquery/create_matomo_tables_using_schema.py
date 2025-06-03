import requests
import psycopg2

# PostgreSQL connection details
PG_HOST = "XXX"
PG_DB = "postgres"
PG_USER = "YYY"
PG_PASSWORD = "ZZZZ"

# GitHub raw content URL for the repo
GITHUB_REPO = "https://raw.githubusercontent.com/innocraft/bigquery-schema/main/"
SCHEMA_NAME = "matomo"  # PostgreSQL schema name where tables will be created

# List of schema files from the GitHub repo
json_files = [
            "log_action.json",
            "log_link_visit_action.json",
            "log_visit.json",
            "site.json",
            "goal.json",
            "log_conversion.json",
            "funnel.json",
            "log_abtesting.json",
            "log_clickid.json",
            "log_conversion_item.json",
            "log_form.json",
            "log_form_field.json",
            "log_form_page.json",
            "log_hsr.json",
            "log_hsr_blob.json",
            "log_hsr_event.json",
            "log_hsr_site.json",
            "log_media.json",
            "log_media_plays.json",
            "site_form.json"
]

# Function to generate PostgreSQL CREATE TABLE statement from BigQuery schema
def convert_to_postgres_schema(schema_json, table_name):
    # Mapping BigQuery types to PostgreSQL types
    type_mapping = {
        "STRING": "TEXT",
        "INTEGER": "INTEGER",
        "FLOAT": "REAL",
        "BOOLEAN": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP",
        "DATE": "DATE"
    }
    columns = []
    for field in schema_json:
        column_name = field['name']
        bq_type = field['type']
        pg_type = type_mapping.get(bq_type, 'TEXT')  # Default to TEXT if type is not in the mapping
        nullable = 'NOT NULL' if field['mode'] == 'REQUIRED' else ''
        columns.append(f"{column_name} {pg_type} {nullable}")
    
    create_table_sql = f"CREATE TABLE {SCHEMA_NAME}.{table_name} (\n" + ",\n".join(columns) + "\n);"
    return create_table_sql

# Function to execute the CREATE TABLE statements in PostgreSQL
def create_table_in_postgres(ddl_statement):
    try:
        conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()
        cur.execute(ddl_statement)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

# Function to fetch JSON schema from GitHub and create table
def process_schema_file(file_name):
    url = GITHUB_REPO + file_name
    try:
        response = requests.get(url)
        response.raise_for_status()
        schema_json = response.json()

        # Extract the table name from the JSON file name (without .json extension)
        table_name = file_name.replace(".json", "")

        # Generate the PostgreSQL DDL
        ddl_statement = convert_to_postgres_schema(schema_json, table_name)
        print(f"Generated DDL for {table_name}:\n{ddl_statement}")

        # Execute the DDL in PostgreSQL
        create_table_in_postgres(ddl_statement)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching schema from GitHub: {e}")

# Main function to loop over the JSON files and create tables
def main():
    for json_file in json_files:
        process_schema_file(json_file)

if __name__ == "__main__":
    main()