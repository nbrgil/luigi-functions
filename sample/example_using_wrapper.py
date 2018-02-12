import datetime

import luigi
from sqlalchemy import create_engine

from postgres_target import PostgresCountTarget

postgres_engine = create_engine("postgresql://postgres:senha@localhost/postgres", pool_size=20)


class ExampleUsingWrapper(luigi.Task):
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


#
class ExampleAllYear(luigi.WrapperTask):
	start = luigi.DateParameter()
	stop = luigi.DateParameter()
	task = luigi.TaskParameter()

	def requires(self):
		date1 = self.start
		date2 = self.stop
		day = datetime.timedelta(days=1)

		# noinspection PyTypeChecker
		while date1 <= date2:
			date1 = date1 + day

			# noinspection PyCallingNonCallable
			yield self.task(date=date1)


if __name__ == '__main__':
	luigi.run(
		cmdline_args=[
			"--start", "2017-01-01", "--stop", "2017-01-30", "--task", "ExampleUsingWrapper"
		],
		main_task_cls=ExampleAllYear,
		local_scheduler=True)


