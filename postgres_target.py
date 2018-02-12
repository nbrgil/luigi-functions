from luigi import Target
from sqlalchemy.engine import Engine


class PostgresCountTarget(Target):
	def __init__(
			self,
			sqlalchemy_engine,
			table_name: str = None,
			sql_filter: str = None,
			schema: str = "public"
	):
		self.sqlalchemy_engine = sqlalchemy_engine  # type: Engine
		self.table_name = table_name
		self.sql_filter = sql_filter
		self.schema = schema

	def exists(self):
		conn = self.sqlalchemy_engine.connect()

		try:
			sql = "SELECT count(*) FROM {}.{} {}".format(
				self.schema, self.table_name, self.sql_filter)
			count = conn.execute(sql).scalar()

			return count > 0

		finally:
			if not conn.closed:
				conn.close()
