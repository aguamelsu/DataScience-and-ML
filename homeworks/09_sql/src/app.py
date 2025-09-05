"""
To run the script, make sure the postgresql service is first started

To start the service:
    $ sudo service postgresql start

To verify if the service is running
    $ sudo service postgresql status
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text, Engine
from dotenv import load_dotenv

# print("USER:", os.getenv("DB_USER"))
# print("PASSWORD:", os.getenv("DB_PASSWORD"))
# print("HOST:", os.getenv("DB_HOST"))
# print("DB NAME:", os.getenv("DB_NAME"))

# Connect to the database with SQLAlchemy
def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    

def run_sql_queries(engine: Engine, filepath: str) -> None:
    # Connect to the file
    with open(filepath, 'r') as file:
        sql_script = file.read()

    # Execute sql queries
    with engine.connect() as connection:
        for query in sql_script.split(";"):
            statement = query.strip()
            if statement:
                connection.execute(text(statement))
        connection.commit()


def main():
    # Load environmental variables
    load_dotenv()

    # Path to sql queries
    create_tables = 'homeworks/09_sql/sql/create.sql'
    insert_data = 'homeworks/09_sql/sql/insert.sql'

    # Connection to engine
    engine = connect()

    # Create and write into the database
    run_sql_queries(engine, create_tables)
    run_sql_queries(engine, insert_data)

    # Read and display table
    publishers = pd.read_sql('SELECT * FROM publishers', engine)
    authors = pd.read_sql('SELECT * FROM authors', engine)
    books = pd.read_sql('SELECT * FROM books', engine)
    book_authors = pd.read_sql('SELECT * FROM book_authors', engine)

    print(publishers, '\n\n')
    print(authors, '\n\n')
    print(books, '\n\n')
    print(book_authors, '\n\n')


if __name__ == "__main__":
    main()


