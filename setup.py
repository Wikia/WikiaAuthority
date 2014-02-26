from setuptools import setup

setup(
    name="wikia_authority",
    version= "0.0.1",
    author = "Robert Elwell",
    author_email = "robert@wikia-inc.com",
    description = "Library for identifying authorship quality in a revision-based system",
    license = "Other",
    packages = ["wikia_authority"],
    depends = [ "requests", "lxml", "cssselect", "python-graph-core", "xlrd", "xlwt",
                "nlp-services>=0.0.1", "python-graph-core"],
    dependency_links=["https://github.com/relwell/nlp_services/archive/master.zip#egg=nlp_services=0.0.1"]
    )