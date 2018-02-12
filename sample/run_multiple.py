import luigi
from sqlalchemy import create_engine

if __name__ == '__main__':
	luigi.run(
		cmdline_args=[
			"--module", "example_task", "RangeDaily", "--of", "ExampleTask", "--start", "2018-01-01",
			"--stop", "2018-01-31"
		],
		local_scheduler=True)
