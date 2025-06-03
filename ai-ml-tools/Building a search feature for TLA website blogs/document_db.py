import sqlite3
import pandas as pd
from sqlalchemy import create_engine


class DocumentManager:
    def __init__(self, db_path):
        self.engine = create_engine(f'sqlite:///{db_path}')
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        """Create a table if it doesn't already exist."""
        table_script = '''
        CREATE TABLE IF NOT EXISTS documents (
            user_id TEXT,
            title TEXT,
            description TEXT,
            document_id INTEGER PRIMARY KEY
        )
        '''
        self.cursor.execute(table_script)
        self.conn.commit()

    def add_documents(self, df):
        """Add multiple documents using a DataFrame."""
        print("df".center(90,'-'))
        print(df)
        print(df.to_sql('documents', self.engine, if_exists='append', index=False))

    def delete_document(self, document_id):
        """Delete a document from the database by document_id."""
        delete_script = 'DELETE FROM documents WHERE document_id = ?'
        self.cursor.execute(delete_script, (document_id,))
        self.conn.commit()

    def update_document(self, user_id, title, description, document_id):
        """Update an existing document."""
        # update_script = '''
        # UPDATE documents
        # SET user_id = ?, title = ?, description = ?
        # WHERE document_id = ?
        # '''
        # self.cursor.execute(update_script, (user_id, title, description, document_id))
        # self.conn.commit()
        updates = []
        parameters = []
        if user_id is not None:
            updates.append("user_id = ?")
            parameters.append(user_id)
        if title is not None:
            updates.append("title = ?")
            parameters.append(title)
        if description is not None:
            updates.append("description = ?")
            parameters.append(description)
        parameters.append(document_id)
        update_script = f'UPDATE documents SET {", ".join(updates)} WHERE document_id = ?'
        self.cursor.execute(update_script, parameters)
        self.conn.commit()

    def fetch_document(self, document_id):
        """Fetch a single document by document_id."""
        fetch_script = 'SELECT * FROM documents WHERE document_id = ?'
        self.cursor.execute(fetch_script, (document_id,))
        return self.cursor.fetchone()

    def fetch_documents_by_user_id(self, user_id):
        """Fetch documents by user_id."""
        query = f"SELECT * FROM documents WHERE user_id = '{user_id}'"
        print("query", query)
        print(self.engine,'engine')
        # Open a connection and fetch data
        # with pd.read_sql_query(query, self.engine) as conn:
        df = pd.read_sql_query(query, self.engine)
        return df.to_dict(orient='records')

    def close(self):

        """Close the database connection."""
        self.conn.close()
