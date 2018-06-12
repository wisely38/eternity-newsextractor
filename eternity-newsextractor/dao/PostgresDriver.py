import configparser
from sqlalchemy import create_engine, MetaData

__all__ = ['PostgresDriver']


class PostgresDriver:

    def __init__(self):
        self.config = configparser.ConfigParser()
        config_files = self.config.read('postgres_config.ini')
        if not config_files:
            self.config['DEFAULT'] = {'user': 'postgres',
                                      'password': '123Postgres!',
                                      'db': 'newspaper',
                                      'host': 'localhost',
                                      'port': '5432'}
            with open('postgres_config.ini', 'w') as configfile:
                self.config.write(configfile)
        self.user = self.config['DEFAULT']['user']
        self.password = self.config['DEFAULT']['password']
        self.db = self.config['DEFAULT']['db']
        self.host = self.config['DEFAULT']['host']
        self.port = self.config['DEFAULT']['port']
        self.db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'

    def create_db(self):
        connection_url = 'postgresql+psycopg2://{}:{}@{}:{}'
        engine, _ = create_engine(connection_url.format(
            self.user, self.password, self.host, self.port, self.db))
        conn = engine.connect()
        conn.execute("commit")
        conn.execute("create database {0}".format(self.db))
        conn.execute("commit")
        conn.close()

    def do_connect(self, user, password, db, host, port):
        if user and password and db and host and port:
            engine, metadata = self.connect(user, password,
                                            db, host, port)
        else:
            engine, metadata = self.connect(self.user, self.password,
                                            self.db, self.host, self.port)
        return engine, metadata, engine.connect()

    def connect_with_retries(self, retries):
        return connect(retries, self.user, self.password,
                       self.db, self.host, self.port)

    def connect_with_retries(self, retries=3, user=None, password=None, db=None, host=None, port=None):
        need_to_retry = True
        while(need_to_retry):
            if retries == 0:
                break
            else:
                need_to_retry = False
            try:
                engine, metadata, connection = self.do_connect(
                    user, password, db, host, port)
            except Exception as db_conn_error:
                need_to_retry = True
                retries = retries - 1
                print(db_conn_error)
                try:
                    self.create_db()
                except Exception as db_create_error:
                    print(db_conn_error)
            # finally:
            #     if connection:
            #         connection.close()
        return engine, metadata, connection

    # def connect(self):
    #     '''Returns a engine and a metadata object'''
    #     return self.connect(self, self.user, self.password,
    #                         self.db, self.host, self.port)

    def connect(self, user, password, db, host, port):
        '''Returns an engine and a metadata object with dynamic configuration'''

        url = self.db_url.format(user, password, host, port, db)

        # The return value of create_engine() is our connection object
        engine = create_engine(url, client_encoding='utf8')

        # We then bind the connection to MetaData()
        meta = MetaData(bind=engine, reflect=True)

        engine.execution_options(
            autocommit=True, autoflush=False, expire_on_commit=False)

        print('Connected to postgres with: user={0}, password={1}, db={2}, host={3}, port={4}'.format(
            user, password, db, host, port))
        return engine, meta