__author__ = 'Marlon'
__version__ = '1.0.0.0'

import sys
sys.path.append("...")
from sqlalchemy import create_engine
import configs.ConfigHandler as conf
pg_conf = conf.get_conf('config.ini','postgres')

class PostgresHandler():

    def __init__(self, db):

        self.db = db
        if db == 'db1':
            try:
                self.database_name = pg_conf['database_1_name']
                self.database_username = pg_conf['database_1_username']
                self.database_password = pg_conf['database_1_password']
                self.database_host = pg_conf['database_1_host']
                self.database_port = pg_conf['database_1_port']
            except Exception as err:
                print (err)

        else:
            try:
                self.database_name = pg_conf['database_2_name']
                self.database_username = pg_conf['database_2_username']
                self.database_password = pg_conf['database_2_password']
                self.database_host = pg_conf['database_2_host']
                self.database_port = pg_conf['database_2_port']
            except Exception as err:
                print (err)
                raise

# try:
#     conn = " "#Todo #psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
# except psycopg2.InterfaceError as err:
#      print exc.message
#      conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
#      pass
# except Exception, err:
#      print err
#      pass
#
# conString_1 = 'postgresql://{}:{}@{}:{}/{}'.format(database_1_username, database_1_password , database_1_host ,database_1_port , database_1_name)
# try:
#     engine_1 = create_engine(conString_1)
#     connection_1 = engine_1.connect()
# except Exception as err:
#     print (err)

    def initiate(self):
        conString = 'postgresql://{}:{}@{}:{}/{}'.format(self.database_username, self.database_password , self.database_host , self.database_port , self.database_name)
        try:
            self.engine = create_engine(conString)
            self.connection = self.engine.connect()
        except Exception as err:
            print (err)

    def execute_query(self, query):

        result = self.connection.execute(query)
        self.engine.dispose()
        columns = result.keys()
        results = []
        if columns:
            for row in result:
                results.append(dict(zip(columns, row)))
        return results

