import datetime
import luigi
from sqlalchemy import create_engine

from postgres_target import PostgresCountTarget

postgres_engine = None


def create_postgres_engine():
	global postgres_engine
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


class ExampleAllYear(luigi.Task):
	year = luigi.IntParameter()
	task = luigi.TaskParameter()

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		create_postgres_engine()

	def requires(self):
		date1 = datetime.date(2017, 1, 1)
		date2 = datetime.date(2017, 12, 31)
		day = datetime.timedelta(days=1)

		while date1 <= date2:
			date1 = date1 + day

			# noinspection PyCallingNonCallable
			yield self.task(date=date1, force=True)


if __name__ == '__main__':
	luigi.run(
		cmdline_args=["--year", "2017", "--task", "ExampleTask"], main_task_cls=ExampleAllYear,
		local_scheduler=True)
