#!usr/bin/env python

""""
Script for initial database setup
"""
import sys
sys.path.insert(0, '/spark_etl')

# local imports
from data_manager.database_handler import PgDb
from etl_conf import conf

class SetupDb(object):
    def __init__(self, configs):
        self.conn = PgDb(configs)

    def create_db_tables(self):
        category_table = """CREATE TABLE if not EXISTS movie_categories(
        id SERIAL PRIMARY KEY,
        category VARCHAR(50) NOT NULL,
        category_id INT NOT NULL
        )
        """

        self.conn.write_query(category_table)

        movie_rank_table = """CREATE TABLE if not exists movie_ranks
        (id SERIAL PRIMARY KEY,
         decade INT NOT NULL,
         category_id INT NOT NULL,
         rank INT NOT NULL,
         movie_id INT NOT NULL,
         movie_name VARCHAR(256)
         )"""

        self.conn.write_query(movie_rank_table)

    def insert_defaults(self):
        pass
    
if __name__ == "__main__":
    ENV = os.getenv('ENVIRONMENT', 'DEV')
    configs = conf[ENV]
    setup_inst = SetupDb(configs.pg_db['pg_data_lake'])
    setup_inst.create_db_tables()
    setup_inst.insert_defaults()