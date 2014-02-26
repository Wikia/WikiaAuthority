from setuptools import setup

setup(
    name="wikia_authority",
    version= "0.0.1",
    author = "Robert Elwell",
    author_email = "robert@wikia-inc.com",
    description = "Library for identifying authorship quality in a revision-based system",
    license = "Other",
    packages = ["wikia_authority"],
    depends = [ "requests", "lxml", "cssselect", "python-graph-core", "xlrd", "xlwt"]
    )