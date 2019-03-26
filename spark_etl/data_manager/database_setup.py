#!usr/bin/env python

""""
Script for initial database setup
"""
import os
import sys
sys.path.insert(0, '/spark_etl')

# local imports
from data_manager.database_handler import PgDb
from etl_conf import conf

class SetupDb(PgDb):
    #def __init__(self, configs):
    #    self.conn = PgDb(configs)

    def create_db_tables(self):
        category_table = """CREATE TABLE if not EXISTS t_movie_category(
        id SERIAL PRIMARY KEY,
        category VARCHAR(50) NOT NULL,
        category_id INT NOT NULL
        )
        """


        self.write_query(category_table)
        logging.info("check/ create t_movie_category done")

        movie_rank_table = """CREATE TABLE if not exists t_movie_rank
        (id SERIAL PRIMARY KEY,
         decade INT NOT NULL,
         category_id INT NOT NULL,
         rank INT NOT NULL,
         movie_id INT NOT NULL,
         movie_name VARCHAR(256)
         )"""

        self.write_query(movie_rank_table)

    def insert_defaults(self):
        categories = [('Crime', 1),
                    ('Romance', 2),
                    ('Thriller', 3),
                    ('Adventure', 4),
                    ('Drama', 5),
                    ('War', 6),
                    ('Documentary', 7),
                    ('Fantasy', 8),
                    ('Mystery', 9),
                    ('Musical', 10),
                    ('Animation', 11),
                    ('Film-Noir', 12),
                    ('(no genres listed)',13),
                    ('IMAX',14),
                    ('Horror', 15),
                    ('Western', 16),
                    ('Comedy', 17),
                    ('Children', 18),
                    ('Action', 19),
                    ('Sci-Fi', 20)]


        records_list = ','.join(['%s'] * len(categories))
        insert_query = 'INSERT INTO t_movie_category (category, category_id) values {}'.format(
            records_list)
        self.write_many_query(insert_query, data)
        logging.info("inserted default values")


if __name__ == "__main__":
    ENV = os.getenv('ENVIRONMENT', 'DEV')
    configs = conf[ENV]
    setup_inst = SetupDb(configs)
    setup_inst.create_db_tables()
    setup_inst.insert_defaults()
