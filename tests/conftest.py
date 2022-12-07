import pyodbc
import pytest
import os
from table_extractor import SqlValidator


@pytest.fixture(scope="session")
def test_odbc_connection_string_in_environment_variables() -> None:
    """To be able to run the SqlValidator tests, a pyodbc database connection is required. The connection string
    is expected in an environment variable SQL_VALIDATOR_CONNECTION_STRING."""
    assert "SQL_VALIDATOR_CONNECTION_STRING" in os.environ.keys()


@pytest.fixture(scope="session")
def db_connection(
    test_odbc_connection_string_in_environment_variables,
) -> pyodbc.Connection:
    """Database connection shared by the individual tests."""
    connection_string = os.getenv("SQL_VALIDATOR_CONNECTION_STRING")
    db_connection = pyodbc.connect(connection_string)
    yield db_connection
    db_connection.close()


@pytest.fixture(scope="session")
def table_catalog() -> str:
    """Central place to store the table catalog name for all the tests."""
    return "IZ_EAM_VAULT_NC.V_ETL_SCOPE"


@pytest.fixture(scope="session")
def github_url_raw() -> str:
    return "https://raw.github.kyndryl.net/michal-sedlacek/table_extractor/master/tests/resources/sysdummy1.sql"


@pytest.fixture(scope="session")
def github_url_web() -> str:
    return "https://github.kyndryl.net/michal-sedlacek/table_extractor/blob/master/tests/resources/sysdummy1.sql"


@pytest.fixture(scope="session")
def github_credentials() -> tuple[str, str]:
    """Read GitHUb username and access token from the environment variables.
    To be able to run the tests from github, GitHub credentials have to be stored in the env variables:
    - GH_USERNAME: the GitHub username
    - GH_TOKEN: the personal GitHub access token"""
    username = os.environ["GH_USERNAME"]
    token = os.environ["GH_TOKEN"]
    return username, token


@pytest.fixture
def sql_validator_sysdummy1(
    db_connection: pyodbc.Connection, table_catalog: str
) -> SqlValidator:
    """Return a SqlValidator object based on sysdummy1.sql file"""
    file_name = os.path.join("resources", "sysdummy1.sql")
    return SqlValidator(
        db_connection=db_connection, table_catalog=table_catalog, file_name=file_name
    )


@pytest.fixture
def sql_validator_mldbnc_mldb_hw_info(
    db_connection: pyodbc.Connection, table_catalog: str
) -> SqlValidator:
    """Return a SqlValidator object based on the MLDBNC_MLDB.HW_INFO table
    (assuming that this table always exists in DB)."""
    sql = f"select * from MLDBNC_MLDB.HW_INFO"
    return SqlValidator(
        db_connection=db_connection, table_catalog=table_catalog, sql=sql
    )
