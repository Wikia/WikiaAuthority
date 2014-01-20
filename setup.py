from setuptools import setup

setup(
    name="wikia_authority",
    version= "0.0.1",
    author = "Robert Elwell",
    author_email = "robert@wikia-inc.com",
    description = "Library for identifying authorship quality in a revision-based system",
    license = "Other",
    packages = ["wikia_authority", "wikia_authority.drivers"],
    depends = ["nlp_services", "requests", "lxml", "nltk", "cssselect", "python-graph-core"]
    )