from table_extractor import TableValidation, TableValidationError, Table
import pytest


def test_create_object():
    # without mandatory parameters
    with pytest.raises(TypeError):
        TableValidation()
    # with mandatory parameters
    schema = "tabschema"
    name = "tabname"
    table = Table(schema=schema, name=name)
    tv = TableValidation(table)
    assert isinstance(tv, TableValidation)
    assert tv.table is table
    assert tv.in_database is False
    assert tv.in_table_catalog is False
    assert tv.update_frequency == ""
    assert tv.update_column == ""
    assert tv.minimum_update_ts is None
    assert tv.next_regular_update_ts is None
    assert tv.actual_update_ts is None
    assert tv.content_current is False
    assert tv.validation_successful is False
    assert tv.validation_error == ""
