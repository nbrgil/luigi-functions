from luigi import Target
import os


class PostgresCountTarget(Target):
    def __init__(
            self, postgres_client: PostgresClient = None, table_name: str = None, sql_filter: str = None,
            force: bool = False, schema: str = 'public'):
        self.postgres_client = postgres_client
        self.table_name = table_name
        self.schema = schema
        self.sql_filter = sql_filter
        self.force = force

    def exists(self):
        if self.force or os.getenv("LUIGIFORCE") == "True":
            return False

        conn = self.postgres_client.get_conn_engine().connect()

        try:
            sql = "SELECT count(*) FROM {}.{} {}".format(
                self.schema, self.table_name, self.sql_filter)
            count = conn.execute(sql).scalar()

            return count > 0

        finally:
            if conn.closed:
                conn.close()


class PostgresLogTarget(Target):
    def __init__(
            self, postgres_client: PostgresClient = None, table_name: str = None, sql_filter: str = None,
            force: bool = False, schema: str = 'zapvivareal'):
        self.postgres_client = postgres_client
        self.table_name = table_name
        self.schema = schema
        self.sql_filter = sql_filter
        self.force = force

    def exists(self):
        if self.force or os.getenv("LUIGIFORCE") == "True":
            return False

        conn = self.postgres_client.get_conn_engine().connect()

        try:
            count = conn.execute(
                "SELECT count(*) FROM {}.{} {}".format(
                    self.schema, self.table_name, self.sql_filter)
            ).scalar()

            return count > 0

        finally:
            if conn.closed:
                conn.close()
