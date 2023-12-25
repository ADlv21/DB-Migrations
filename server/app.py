from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Connect to PostgreSQL and MySQL databases
postgres_engine = create_engine('postgresql://admin:admin@localhost/postgres_database')
mysql_engine = create_engine('mysql+mysqlconnector://username:password@localhost/mysql_database')

# Create sessions for PostgreSQL and MySQL
postgres_session = sessionmaker(bind=postgres_engine)()
mysql_session = sessionmaker(bind=mysql_engine)()

# Reflect PostgreSQL schema
postgres_metadata = MetaData(bind=postgres_engine)
postgres_metadata.reflect()

# Create corresponding MySQL schema
mysql_metadata = MetaData(bind=mysql_engine)

# Track tables that have been created to avoid re-creating them
created_tables = set()

for table_name, postgres_table in postgres_metadata.tables.items():
    # Create corresponding MySQL table if not created before
    if table_name not in created_tables:
        mysql_table = Table(table_name, mysql_metadata, *[c.copy() for c in postgres_table.columns])
        mysql_metadata.create_all()
        created_tables.add(table_name)

    # Query data from PostgreSQL
    postgres_data = postgres_session.query(postgres_table).all()

    # Insert data into MySQL
    mysql_table = Table(table_name, mysql_metadata, autoload=True)
    for row in postgres_data:
        mysql_session.execute(mysql_table.insert().values(row._asdict()))

# Commit the changes
mysql_session.commit()

# Close sessions
postgres_session.close()
mysql_session.close()
