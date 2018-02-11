import datetime

import luigi
from luigi.contrib import postgres


class ExampleTask(luigi.Task):
    date = luigi.DateParameter()

    def run(self):
        print("1234")
        print(self.date)

    def output(self):
        return postgres.PostgresTarget(
            host="localhost:5432", database="postgres", user="postgres", password="senha",
            table="temp", update_id=str(self.date))


class ExampleAllYear(luigi.Task):
    year = luigi.IntParameter()
    task = luigi.TaskParameter()

    def requires(self):
        date1 = datetime.date(2017, 1, 1)
        date2 = datetime.date(2017, 12, 31)
        day = datetime.timedelta(days=1)

        while date1 <= date2:
            date1 = date1 + day

            # noinspection PyCallingNonCallable
            yield self.task(date=date1)


if __name__ == '__main__':
    luigi.run(
        cmdline_args=["--year", "2017", "--task", "ExampleTask"], main_task_cls=ExampleAllYear,
        local_scheduler=True)
