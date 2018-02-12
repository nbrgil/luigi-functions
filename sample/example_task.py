import luigi
from sqlalchemy import create_engine

from postgres_target import PostgresCountTarget
postgres_engine = create_engine("postgresql://postgres:senha@localhost/postgres", pool_size=20)


class ExampleTask(luigi.Task):
	date = luigi.DateParameter()
	force = luigi.BoolParameter(significant=False, default=False)

	def __init__(self, *args, **kwargs):

		super().__init__(*args, **kwargs)
		if self.force:
			self.clean()

	def clean(self):
		conn = postgres_engine.connect()

		try:
			sql = "DELETE FROM {} {}".format(
				"tabela_exemplo", "where data = '{}'".format(self.date))
			conn.execute(sql)

		finally:
			if not conn.closed:
				conn.close()

	@classmethod
	def bulk_complete(cls, parameter_tuples):

		conn = postgres_engine.connect()
		result = []

		try:
			sql = "SELECT distinct data FROM {} WHERE data in :date_list".format("tabela_exemplo")
			result = conn.execute(sql, date_list=parameter_tuples)

		finally:
			if not conn.closed:
				conn.close()
			return result

	def run(self):
		conn = postgres_engine.connect()

		try:
			conn.execute("INSERT INTO tabela_exemplo VALUES ('{}', '{}')".format(self.date, "texto"))
		finally:
			if not conn.closed:
				conn.close()

	def output(self):
		return PostgresCountTarget(
			sqlalchemy_engine=postgres_engine,
			table_name="tabela_exemplo",
			sql_filter="where data = '{}'".format(self.date)
		)