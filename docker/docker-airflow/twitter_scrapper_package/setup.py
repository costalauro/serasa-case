from setuptools import setup, find_packages

setup(
    name="twitter_scrapper",
    version="0.1.0",
    description="Twitter Scrapper package",
    author="Lauro Costa Bulhões",
    author_email="costalauro@hotmail.com",
    url="",
    packages=find_packages(exclude=("tests",)),
    python_requires=">=3.7",
)