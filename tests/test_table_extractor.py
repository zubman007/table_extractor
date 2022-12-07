import pandas as pd
import pytest
import os

from table_extractor import TableExtractor, TableExtractorError, Table


def test_create_object():
    te = TableExtractor()
    assert isinstance(te, TableExtractor)
    assert te.sql == ""
    assert te.url == ""
    assert te.file_name == ""
    assert te.tables == []


def test_from_file():
    # test non-existing file
    sa = TableExtractor()
    with pytest.raises(TableExtractorError):
        sa.from_file("non-existing-file")
    # test existing file
    file_name = os.path.join("resources", "sysdummy1.sql")
    sa.from_file(file_name)
    assert sa.file_name == file_name
    assert sa.sql == "select 1 from sysibm.sysdummy1"


def test_remove_comments_single_line():
    sql = """-- initial comment;
select * from schema.table;-- in-line comment
select * from schema.table2;-- in-line comment
-- middle comment
select * from schema.table3;-- in-line comment
-- closing comment;"""
    expected = """
select * from schema.table;
select * from schema.table2;

select * from schema.table3;
"""
    result = TableExtractor._remove_comments(sql)
    assert result == expected


def test_remove_multi_line_comments():
    sql = """something /* comment 
comment
comment */
something more
/* another comment */
something else"""
    expected = """something 
something more

something else"""
    result = TableExtractor._remove_comments(sql)
    assert result == expected


def test_remove_blank_lines():
    assert TableExtractor._remove_blank_lines("\r\n") == ""
    assert TableExtractor._remove_blank_lines("\n") == ""
    assert TableExtractor._remove_blank_lines("    \n") == ""
    assert TableExtractor._remove_blank_lines("\t\n") == ""


def test_trim_lines():
    sql = "    \t test  \n" * 3
    expected = "test\n" * 3
    result = TableExtractor._trim_lines(sql)
    assert result == expected


def test_identify_source_tables():
    # standard query
    sql = """select * 
    fRom schEma-1 . Table_2 t2 
    jOin schemA-1.Table_3 as t3 
    where a = 1"""
    expected = {"SCHEMA-1.TABLE_2", "SCHEMA-1.TABLE_3"}
    result = TableExtractor._identify_source_tables(sql)
    assert expected == result
    # trim from
    sql = "select trim(' ' from a.col) from new.table"
    expected = {"NEW.TABLE"}
    result = TableExtractor._identify_source_tables(sql)
    assert expected == result
    # extract from
    sql = "select extract(day from a.col) from new.table"
    expected = {"NEW.TABLE"}
    result = TableExtractor._identify_source_tables(sql)
    assert expected == result


def test_identify_target_tables():
    sql = """ create   table  new.table as
    select * 
    fRom schEma-1 . Table_2 t2 
    jOin schemA-1.Table_3 as t3 
    where a = 1;
    create  hadoop   table new.table2 as select * from new.table;"""
    expected = {"NEW.TABLE", "NEW.TABLE2"}
    result = TableExtractor._identify_target_tables(sql)
    assert expected == result


def test_identify_renamed_tables():
    sql = """  create  hadoop   table New.Table2 as select * from new.table;
    rename   table  nEw . table2 to Table3"""
    expected = {("NEW.TABLE2", "NEW.TABLE3")}
    result = TableExtractor._identify_renamed_tables(sql)
    assert expected == result


def test_identify_populated_tables():
    # insert into
    sql = """  insert into New.Table2 select * from new.table; """
    expected = {"NEW.TABLE2"}
    result = TableExtractor._identify_populated_tables(sql)
    assert expected == result
    # load hadoop
    sql = """  load hadoop xxx into table New.Table2 overwrite; """
    expected = {"NEW.TABLE2"}
    result = TableExtractor._identify_populated_tables(sql)
    assert expected == result


def test_analyze_simple_source_table():
    sql = "select 1 from sysibm.sysdummy1"
    sa = TableExtractor(sql)
    sa.analyze()
    assert len(sa.tables) == 1
    table = sa.tables[0]
    assert isinstance(table, Table)
    assert table.full_name == "SYSIBM.SYSDUMMY1"
    assert table.used is True
    assert table.created is False
    assert table.renamed is False
    assert table.sql == ""


def test_analyze_source_target():
    sql = """ create table new.table as 
    select * from old.table ot 
    join other.table jt  on jt.c = ot.c 
    left join other.table2 jt2 on jt.c = jt2.c
    where ot.cc = "x"; """
    sa = TableExtractor(sql)
    sa.analyze()
    source_tables = [
        Table(schema="OLD", name="TABLE", used=True),
        Table(schema="OTHER", name="TABLE", used=True),
        Table(schema="OTHER", name="TABLE2", used=True),
    ]
    target_table = Table(
        schema="NEW",
        name="TABLE",
        created=True,
        sql=TableExtractor._trim_lines(sql).replace(";", ""),
        parent_tables=source_tables.copy(),
    )
    all_tables = source_tables.copy()
    all_tables.append(target_table)
    expected_tables = {t.full_name: t for t in all_tables}
    assert len(sa.tables) == len(expected_tables)
    for table in sa.tables:
        assert table.full_name in expected_tables.keys()
        assert table == expected_tables[table.full_name]


def test_analyze_rename_table():
    sql = " rename  table  schema.old  to  new; "
    sa = TableExtractor(sql)
    sa.analyze()
    source_table = Table(schema="SCHEMA", name="OLD", used=True)
    target_table = Table(
        schema="SCHEMA",
        name="NEW",
        renamed=True,
        sql=TableExtractor._trim_lines(sql).replace(";", ""),
        parent_tables=[source_table],
    )
    expected_tables = [source_table, target_table]
    expected_tables = {t.full_name: t for t in expected_tables}
    assert len(sa.tables) == 2
    for t in sa.tables:
        assert t.full_name in expected_tables.keys()
        assert t == expected_tables[t.full_name]


def test_tables_to_data_frame():
    sql = "create table new.table as select * from old.table"
    sa = TableExtractor(sql)
    sa.analyze()
    result = sa.tables_to_data_frame()
    # built the expected dataframe output
    columns = [
        "SCHEMA",
        "NAME",
        "FULL_NAME",
        "USED",
        "CREATED",
        "RENAMED",
        "POPULATED",
        "SQL",
        "PARENT_TABLES",
    ]
    index = [0, 1]
    data = [
        ["OLD", "TABLE", "OLD.TABLE", True, False, False, False, "", []],
        ["NEW", "TABLE", "NEW.TABLE", False, True, False, False, sql, [0]],
    ]
    expected = pd.DataFrame(data, columns=columns, index=index)
    assert result.equals(expected)


def test_tables_to_edge_data_frame():
    te = TableExtractor()
    te.from_file(os.path.join("resources", "3_joined_tables.sql"))
    te.analyze()
    result = te.tables_to_edge_data_frame()
    result = result.sort_values("SOURCE").reset_index(drop=True)
    data = [
        ["TABSCHEMA.TABLE1", "TABSCHEMA.NEW_TABLE"],
        ["TABSCHEMA.TABLE2", "TABSCHEMA.NEW_TABLE"],
        ["TABSCHEMA.TABLE3", "TABSCHEMA.NEW_TABLE"],
    ]
    expected = pd.DataFrame(data, columns=["SOURCE", "TARGET"])
    assert result.equals(expected)


def test_tables_relationship_chart():
    te = TableExtractor()
    te.from_file(os.path.join("resources", "3_joined_tables.sql"))
    te.analyze()
    te.tables_relationship_chart()


def test_from_github(github_url_raw, github_credentials):
    """Prerequisite: a GitHub personal access token have to be stored in the GH_TOKEN environment variable."""
    te = TableExtractor()
    username, token = github_credentials
    # wrong URL
    with pytest.raises(TableExtractorError):
        te.from_github("wrong url", user_name=username, access_token=token)
    # real URL
    te.from_github(file_url=github_url_raw, user_name=username, access_token=token)
    assert te.url == github_url_raw
    assert te.sql == "select 1 from sysibm.sysdummy1"


def test__clean_github_url():
    te = TableExtractor()
    # raw format with a token - the token should be removed
    url = "https://raw.github.kyndryl.net/michal-sedlacek/table_extractor/master/tests/resources/sysdummy1.sql?token=AAANUAH43UXXHX6IN4WFEHTCQYMVO"
    expected = "https://raw.github.kyndryl.net/michal-sedlacek/table_extractor/master/tests/resources/sysdummy1.sql"
    assert te._clean_github_url(url) == expected
    # raw format without a token - no change
    url = "https://raw.github.kyndryl.net/michal-sedlacek/table_extractor/master/tests/resources/sysdummy1.sql"
    assert te._clean_github_url(url) == url
    # web format - expected to be converted to the raw format
    url = "https://github.kyndryl.net/michal-sedlacek/table_extractor/blob/master/tests/resources/sysdummy1.sql"
    expected = "https://raw.github.kyndryl.net/michal-sedlacek/table_extractor/master/tests/resources/sysdummy1.sql"
    assert te._clean_github_url(url) == expected
