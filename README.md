# datadrivet_hemnet_scraper

The main analysis for this project can be found in the notebook called `analysis.ipynb`.

In this project I used Dagster, which is a data orchestration tool, to break up the different data transformation that I was doing during the scraping. It worked really well for this application as I would not have to re-run the entire pipeline when I discovered an error in one of the steps I was doing. This was especially helpful when fetching the data would take up to 20 minutes, with a traditional script, I would have to rerun that entire 20 minutes of scraping everytime I would discover an error. Pretty neat!

# Dagster Docs

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `datadrivet_hemnet_scraper/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development


### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `datadrivet_hemnet_scraper_tests` directory and you can run tests using `pytest`:

```bash
pytest datadrivet_hemnet_scraper_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
