import pytest
import os
import pandas as pd
from datetime import datetime
from table_extractor import (
    SqlValidator,
    SqlValidatorError,
    TableExtractor,
    TableValidation,
)


def test_create_object(db_connection, table_catalog):
    sql = "select * from sysibm.dummy1"
    # without mandatory parameters
    with pytest.raises(TypeError):
        SqlValidator()
    # with an invalid table catalog name
    invalid_table_catalogs = [
        "",
        "with spaces",
        "WithoutDot",
        "too.many.dots",
        "question?mark.IsWrong",
    ]
    for tc in invalid_table_catalogs:
        with pytest.raises(SqlValidatorError):
            SqlValidator(db_connection=db_connection, table_catalog=tc, sql=sql)
    # non-existing table catalog name (but in the right format)
    with pytest.raises(SqlValidatorError):
        SqlValidator(db_connection=db_connection, table_catalog="NOT.EXISTS", sql=sql)
    # without sql
    with pytest.raises(SqlValidatorError):
        SqlValidator(db_connection=db_connection, table_catalog=table_catalog)
    # with correct mandatory parameters
    se = SqlValidator(db_connection=db_connection, table_catalog=table_catalog, sql=sql)
    assert isinstance(se, SqlValidator)
    assert se.db_connection is db_connection
    assert se.table_catalog == table_catalog
    assert isinstance(se.table_extractor, TableExtractor)
    assert se.sql == sql
    assert se.file_name == ""
    assert se.url == ""
    assert isinstance(se.table_validations, list)
    assert len(se.table_validations) == len(se.table_extractor.tables)
    for tv in se.table_validations:
        assert isinstance(tv, TableValidation)
    assert len(se.table_validations) == len(se._table_cash)


def test_create_from_github(
    github_url_raw, github_url_web, db_connection, table_catalog, github_credentials
):
    """Prerequisite: a GitHub personal access token have to be stored in the GH_TOKEN environment variable."""
    username, token = github_credentials
    # wrong URL
    with pytest.raises(SqlValidatorError):
        SqlValidator(
            url="wrong url",
            github_username=username,
            github_access_token=token,
            db_connection=db_connection,
            table_catalog=table_catalog,
        )
    # github raw url
    sv = SqlValidator(
        url=github_url_raw,
        github_username=username,
        github_access_token=token,
        db_connection=db_connection,
        table_catalog=table_catalog,
    )
    assert sv.url == github_url_raw
    assert sv.sql == "select 1 from sysibm.sysdummy1"
    # github raw url
    sv = SqlValidator(
        url=github_url_web,
        github_username=username,
        github_access_token=token,
        db_connection=db_connection,
        table_catalog=table_catalog,
    )
    assert sv.url == github_url_raw
    assert sv.sql == "select 1 from sysibm.sysdummy1"


def test_from_file(db_connection, table_catalog, github_credentials):
    username, token = github_credentials
    # test non-existing file
    with pytest.raises(SqlValidatorError):
        SqlValidator(
            file_name="non-existing-file",
            github_username=username,
            github_access_token=token,
            db_connection=db_connection,
            table_catalog=table_catalog,
        )
    # test existing file
    file_name = os.path.join("resources", "sysdummy1.sql")
    sv = SqlValidator(
        file_name=file_name,
        github_username=username,
        github_access_token=token,
        db_connection=db_connection,
        table_catalog=table_catalog,
    )
    assert sv.file_name == file_name
    assert sv.sql == "select 1 from sysibm.sysdummy1"


# data for test__check_table_name_format
table_name_formats = [
    ("schema.table", True),
    ("table", False),
    ("", False),
    ("extra.schema.table", False),
]


@pytest.mark.parametrize("table_name,result", table_name_formats)
def test__check_table_name_format(table_name, result):
    assert SqlValidator._check_table_name_format(table_name) == result


# data for test__check_valid_characters
valid_characters = [
    ("aA0-_.", True),
    ("a b", False),
    ("a?b", False),
    ('a"b', False),
    ("a'b", False),
]


@pytest.mark.parametrize("text,result", valid_characters)
def test__check_valid_characters(text, result):
    assert SqlValidator._check_valid_characters(text) == result


def test_table_names_fully_qualified(sql_validator_sysdummy1):
    table_names = sql_validator_sysdummy1.table_names_fully_qualified
    assert isinstance(table_names, list)
    assert len(table_names) == 1
    assert table_names[0] == "SYSIBM.SYSDUMMY1"


def test_table_names_tuple(sql_validator_sysdummy1):
    table_names = sql_validator_sysdummy1.table_names_tuples
    assert isinstance(table_names, list)
    assert len(table_names) == 1
    assert isinstance(table_names[0], tuple)
    assert table_names[0] == ("SYSIBM", "SYSDUMMY1")


def test__check_table_existence(
    sql_validator_sysdummy1,
    sql_validator_mldbnc_mldb_hw_info,
    db_connection,
    table_catalog,
):
    # table existing in the DB
    assert sql_validator_sysdummy1.table_validations[0].in_database is False
    # table not existing in the DB
    assert sql_validator_mldbnc_mldb_hw_info.table_validations[0].in_database is True
    # no tables
    sql_validator_sysdummy1.table_validations = []
    sql_validator_sysdummy1._table_cash = {}
    sql_validator_sysdummy1._check_table_existence()


def test__read_table_catalog(
    sql_validator_sysdummy1, sql_validator_mldbnc_mldb_hw_info
):
    sv = sql_validator_sysdummy1
    assert sv.table_validations[0].in_table_catalog is False
    assert sv.table_validations[0].update_frequency == ""
    assert sv.table_validations[0].update_column == ""
    assert sv.table_validations[0].minimum_update_ts == None
    assert sv.table_validations[0].next_regular_update_ts == None
    sv = sql_validator_mldbnc_mldb_hw_info
    assert sv.table_validations[0].in_table_catalog is True
    assert sv.table_validations[0].update_frequency == "D"
    assert sv.table_validations[0].update_column == "UPLOAD"
    assert sv.table_validations[0].minimum_update_ts is not None
    assert sv.table_validations[0].next_regular_update_ts is not None


def monkey_patch_sql_validator_update_column(
    sv: SqlValidator, update_column: str
) -> None:
    # Repetitive steps for test__check_table_content
    sv.table_validations[0].update_column = update_column
    sv.table_validations[0].validation_error = ""
    sv.table_validations[0].actual_update_ts = None
    sv.table_validations[0].content_current = False
    sv._check_table_content()


def test__check_table_content(
    sql_validator_sysdummy1, sql_validator_mldbnc_mldb_hw_info, table_catalog
):
    # table not in the database
    sv = sql_validator_sysdummy1
    assert sv.table_validations[0].in_database is False
    assert sv.table_validations[0].actual_update_ts is None
    assert sv.table_validations[0].content_current is False
    assert "not found in the database" in sv.table_validations[0].validation_error
    # table not in the table catalog

    # assert "not registered" in sv.table_validations[0].validation_error
    # assert table_catalog in sv.table_validations[0].validation_error
    # table exists in the table catalog
    sv = sql_validator_mldbnc_mldb_hw_info
    assert sv.table_validations[0].in_table_catalog is True
    assert isinstance(sv.table_validations[0].actual_update_ts, datetime)
    # outdated_data
    sv.table_validations[0].validation_error = ""
    sv.table_validations[0].minimum_update_ts = datetime(2100, 1, 1)  # in the future
    sv.table_validations[0].actual_update_ts = None
    sv.table_validations[0].content_current = False
    sv._check_table_content()
    assert "outdated" in sv.table_validations[0].validation_error
    assert sv.table_validations[0].content_current == False
    # current data
    sv.table_validations[0].validation_error = ""
    sv.table_validations[0].minimum_update_ts = datetime(1901, 1, 1)  # in the past
    sv.table_validations[0].actual_update_ts = None
    sv.table_validations[0].content_current = False
    sv._check_table_content()
    assert sv.table_validations[0].validation_error == ""
    assert sv.table_validations[0].content_current == True
    # blank update_column name
    monkey_patch_sql_validator_update_column(sv, "")
    assert "blank" in sv.table_validations[0].validation_error
    assert sv.table_validations[0].actual_update_ts is None
    assert sv.table_validations[0].content_current is False
    # update_column with invalid characters
    monkey_patch_sql_validator_update_column(sv, 'UPLOAD"')
    assert "invalid" in sv.table_validations[0].validation_error
    assert sv.table_validations[0].actual_update_ts is None
    assert sv.table_validations[0].content_current is False
    # non-existing update_column
    monkey_patch_sql_validator_update_column(sv, "NON_EXISTING_COLUMN")
    assert "unable" in sv.table_validations[0].validation_error
    assert "query" in sv.table_validations[0].validation_error
    assert sv.table_validations[0].actual_update_ts is None
    assert sv.table_validations[0].content_current is False
    # update column with incorrect data type
    monkey_patch_sql_validator_update_column(sv, "ADDRESS_REF")
    assert "str" in sv.table_validations[0].validation_error
    assert sv.table_validations[0].actual_update_ts is None
    assert sv.table_validations[0].content_current is False


def test_to_data_frame(sql_validator_mldbnc_mldb_hw_info):
    result = sql_validator_mldbnc_mldb_hw_info.to_data_frame()
    # just test the output format
    assert isinstance(result, pd.DataFrame)


def test_validation_successful(
    sql_validator_sysdummy1, sql_validator_mldbnc_mldb_hw_info
):
    sv = sql_validator_sysdummy1
    sv2 = sql_validator_mldbnc_mldb_hw_info
    # append TableValidation from the other object, in order to have 2 items in the list
    sv.table_validations.append(sv2.table_validations[0])
    # False, False -> False
    sv.table_validations[0].validation_successful = False
    sv.table_validations[1].validation_successful = False
    assert sv.validation_successful is False
    # True, False -> False
    sv.table_validations[1].validation_successful = True
    assert sv.validation_successful is False
    # True, True -> True
    sv.table_validations[0].validation_successful = True
    assert sv.validation_successful is True
    # no Table Validation objects -> False
    sv.table_validations = []
    assert sv.validation_successful is False
