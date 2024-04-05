from sqlalchemy import create_engine, MetaData, Table, select, Column, text

class DatabaseMigrationService:
    def __init__(self, source_params, target_params):
        self.source_engine = create_engine(source_params)
        self.target_engine = create_engine(target_params)
        self.source_metadata = MetaData()
        self.target_metadata = MetaData()
    
    def extract_tables(self):
        self.source_metadata.reflect(bind=self.source_engine)
        return self.source_metadata.tables.keys()
    
    def extract_columns(self, table_name):
        table = Table(table_name, self.source_metadata, autoload=True, autoload_with=self.source_engine)
        return {column.name: column.type for column in table.columns}
    
    def extract_data(self, table_name):
        table = Table(table_name, self.source_metadata, autoload=True, autoload_with=self.source_engine)
        with self.source_engine.connect() as connection:
            select_stmt = select(table)
            result = connection.execute(select_stmt)
            return result.fetchall()
    
    def create_table(self, table_name, columns):
        if not self.target_metadata.tables.get(table_name, None):
            Table(table_name, self.target_metadata, *[Column(name, type_) for name, type_ in columns.items()]).create(bind=self.target_engine)
        else:
            print(f"Table '{table_name}' already exists, skipping creation.")
            
    def insert_data(self, table_name, columns, data):
        with self.target_engine.connect() as connection:
            column_names = list(columns.keys())
            sql_stmt = text(f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join([':' + name for name in column_names])})")
            print('sql_stmt :', sql_stmt)
            print('data :', data)
            connection.execute(sql_stmt, data)
            connection.commit()
    
    def migrate_data(self):
        tables = self.extract_tables()
        for table in tables:
            columns = self.extract_columns(table)
            data = self.extract_data(table)
            data_as_dicts = [dict(zip(columns, row)) for row in data]
            self.create_table(table, columns)
            self.insert_data(table, columns, data_as_dicts)

source_params = "postgresql://postgres:@localhost/local149"
target_params = "postgresql://postgres:@localhost/mgrdb"

migration_service = DatabaseMigrationService(source_params, target_params)
migration_service.migrate_data()
