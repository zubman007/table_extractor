from setuptools import setup


setup(
    name="table_extractor",
    version="0.3",
    packages=["table_extractor"],
    install_requires=["pandas", "requests", "pyodbc"],
    zip_safe=False,
)
