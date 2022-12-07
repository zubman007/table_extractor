from table_extractor import Table
import pytest


def test_table_constractor():
    # without mandatory parameters
    with pytest.raises(TypeError):
        Table()
    # with parameters
    schema = "tabschema"
    name = "tabname"
    t = Table(schema=schema, name=name)
    assert isinstance(t, Table)


def test_table_fields():
    schema = "tabschema"
    name = "tabname"
    t = Table(schema=schema, name=name)
    assert t.schema == schema
    assert t.name == name
    assert t.sql == ""
    assert t.used is False
    assert t.created is False
    assert t.renamed is False
    assert t.populated is False
    assert t.parent_tables == []
    assert t.parent_table_names == []
    assert t.full_name == f"{schema}.{name}"


def test_parent_table_names():
    t = Table(schema="SCHEMA", name="NAME")
    data = [["SCHEMA2", "NAME2"], ["SCHEMA1", "NAME1"]]
    parent_tables = []
    for d in data:
        parent_tables.append(Table(schema=d[0], name=d[1]))
    # monkey patch table t with parent_tables
    t.parent_tables = parent_tables
    expected = ["SCHEMA1.NAME1", "SCHEMA2.NAME2"]
    result = t.parent_table_names
    assert result == expected
