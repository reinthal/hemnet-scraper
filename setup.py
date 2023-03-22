from setuptools import find_packages, setup

setup(
    name="datadrivet_hemnet_scraper",
    packages=find_packages(exclude=["datadrivet_hemnet_scraper_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
