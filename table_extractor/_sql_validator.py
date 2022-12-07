from __future__ import annotations
from table_extractor._table_extractor import TableExtractor, TableExtractorError
from table_extractor._table_validation import TableValidation
from dataclasses import dataclass, field
import re
import pyodbc
from datetime import datetime
import pandas as pd


@dataclass
class SqlValidator:
    """Validate the provided SQL statement and indicate if it should be executed or not.

    Required parameters:
    - db_connection - pyodbc database Connection where the data are stored
    - table_catalog - the fully qualified db table / view name with the meta information about tables

    The SQL statement can be provided by different methods:
    - as a string (sql)
    - from a file for (from_file)
    - from a file stored on GitHub (url), also the github_username and github_access_token have to be provided

    The following validations are performed:
    - identify tables used in the SQL statement and their relationship
    - check if the tables are registered in the table catalog
    - check if the tables used as a source exist in the database
    - check if the source table content is fresh (using the update column from the table catalog)
    - check if the target table is already refreshed

    The results can be obtained as:
    - to_data_frame() - the full details on the table-level in the DataFrame format
    """

    db_connection: pyodbc.Connection
    table_catalog: str
    table_extractor: TableExtractor | None = None
    sql: str = ""
    file_name: str = ""
    url: str = ""
    github_username: str = ""
    github_access_token: str = ""
    table_validations: list[TableValidation] = field(default_factory=list)
    _table_cash: dict[str:TableValidation] = field(default_factory=dict)

    def __post_init__(self):
        """Consolidate SQL statement sources, create table_extractor and validate parameters"""
        # check if any SQL ha been provided, create table extractor object
        if self.sql != "":
            try:
                self.table_extractor = TableExtractor(self.sql)
            except TableExtractorError as e:
                raise SqlValidatorError(f"Unable to parse sql parameter. {str(e)}")
        elif self.file_name != "":
            try:
                self.table_extractor = TableExtractor()
                self.table_extractor.from_file(self.file_name)
                self.sql = self.table_extractor.sql
            except TableExtractorError as e:
                raise SqlValidatorError(f"Unable open file {self.file_name}. {str(e)}")
        elif self.url != "":
            try:
                self.table_extractor = TableExtractor()
                self.table_extractor.from_github(
                    self.url, self.github_username, self.github_access_token
                )
                self.sql = self.table_extractor.sql
                self.url = (
                    self.table_extractor.url
                )  # get back the cleaned url (raw vs. web url format)
            except TableExtractorError as e:
                raise SqlValidatorError(f"Unable to read from url {self.url}. {str(e)}")
        else:
            message = "No SQL statement has been provided. Provide either sql, file_name or url."
            raise SqlValidatorError(message)
        # run table extractor analyzes and create table validation list
        self.table_extractor.analyze()
        for table in self.table_extractor.tables:
            table_validation = TableValidation(table)
            self.table_validations.append(table_validation)
            self._table_cash[table.full_name] = table_validation
        # table_catalog - convert to upper case and validate
        self.table_catalog = self.table_catalog.upper()
        if self._check_valid_characters(self.table_catalog) is False:
            message = f"The provided table_catalog name contain invalid characters: {self.table_catalog}"
            raise SqlValidatorError(message)
        if self._check_table_name_format(self.table_catalog) is False:
            message = f"The table_catalog name has been provided in a wrong format: {self.table_catalog}."
            message += "\nThe expected format is TABSCHEMA.TABNAME!"
            raise SqlValidatorError(message)
        # check if the table_catalog table exists in the database
        try:
            sql = f"select top 1 1 from {self.table_catalog}"
            self.db_connection.execute(sql)
        except pyodbc.ProgrammingError as e:
            message = f"Can't connect to the provided table catalog table: {self.table_catalog}"
            message += str(e)
            raise SqlValidatorError(e)
        # validate data
        self._check_table_existence()
        self._read_table_catalog()
        self._check_table_content()

    def to_data_frame(self) -> pd.DataFrame:
        """Export validation details in a DataFrame format."""
        data = []
        table_cash = {}
        # map table names to their future indexes in the dataframe
        for idx, tv in enumerate(self.table_validations):
            table_cash[tv.table.full_name] = idx
        # loop through the tables
        for tv in self.table_validations:
            # build a list of parent table indexes
            parent_tables = []
            for pt in tv.table.parent_tables:
                parent_tables.append(table_cash[pt.full_name])
            # add data row
            data.append(
                [
                    tv.table.schema,
                    tv.table.name,
                    tv.table.full_name,
                    tv.table.used,
                    tv.table.created,
                    tv.table.renamed,
                    tv.table.sql,
                    parent_tables,
                    tv.in_database,
                    tv.in_table_catalog,
                    tv.update_column,
                    tv.update_frequency,
                    tv.minimum_update_ts,
                    tv.next_regular_update_ts,
                    tv.actual_update_ts,
                    tv.content_current,
                    tv.validation_successful,
                    tv.validation_error,
                ]
            )
        # create the dataframe
        index = range(len(self.table_validations))
        columns = [
            "SCHEMA",
            "NAME",
            "FULL_NAME",
            "USED",
            "CREATED",
            "RENAMED",
            "SQL",
            "PARENT_TABLES",
            "IN_DATABASE",
            "IN_TABLE_CATALOG",
            "UPDATE_COLUMN",
            "UPDATE_FREQUENCY",
            "MINIMUM_UPDATE_TS",
            "NEXT_REGULAR_UPDATE_TS",
            "ACTUAL_UPDATE_TS",
            "CONTENT_CURRENT",
            "VALIDATION_SUCCESSFUL",
            "VALIDATION_ERROR",
        ]
        df = pd.DataFrame(data, columns=columns, index=index)
        return df

    @staticmethod
    def _check_table_name_format(table_name: str) -> bool:
        """Check if the table_name is provided in the format of TABSCHEMA.TABNAME."""
        match = re.match("^[A-Z\d_\-]+\.[A-Z\d_\-]+$", table_name, flags=re.IGNORECASE)
        return match is not None

    @staticmethod
    def _check_valid_characters(table_name: str) -> bool:
        """Check if the table_name is provided without any invalid characters.
        The valid characters are letters, digits, underscore, dash and dot."""
        invalid_characters = re.findall(
            "[^A-Z\d_\-\.]", table_name, flags=re.IGNORECASE
        )
        return len(invalid_characters) == 0

    def _check_table_existence(self) -> None:
        """Check in the database if the tables exist there or not and update the in_database flag
        for each TableValidation object."""
        if not self.table_validations:
            return  # no tables identified in the SQL
        # build query to check table existence in sys.tables
        sql_snippets = [
            f"(upper(s.name)='{t[0]}' and upper(t.name)='{t[1]}')"
            for t in self.table_names_tuples
        ]
        sql = f"""select  upper(s.name) + '.' + upper(t.name) 
from    sys.tables  t 
join    sys.schemas s   on t.schema_id = s.schema_id 
where   {" or ".join(sql_snippets)}
union
select  upper(s.name) + '.' + upper(t.name) 
from    sys.views   t
join    sys.schemas s   on t.schema_id = s.schema_id 
where   {" or ".join(sql_snippets)}
"""
        # execute query
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            # update DataValidation objects
            for row in results:
                self._table_cash[row[0]].in_database = True
        except pyodbc.ProgrammingError as e:
            raise SqlValidatorError(f"Error when executing SQL: {str(e)}{chr(10)}{sql}")

    def _read_table_catalog(self):
        """Read table catalog data and update TableValidation objects"""
        if not self.table_validations:
            return  # no tables identified in the SQL
        # build a query to read data for all tables from the table catalog
        sql_snippets = [
            f"(upper(object_schema_name)='{t[0]}' and upper(object_name)='{t[1]}')"
            for t in self.table_names_tuples
        ]
        sql = f"""select  upper(object_schema_name) + '.' + upper(object_name)
        , object_update_frequency
        , object_update_column
        , object_minimum_update
        , object_next_regular_update
from    {self.table_catalog}
where   {" or ".join(sql_snippets)}"""
        # execute query
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            # update DataValidation objects
            for row in results:
                self._table_cash[row[0]].in_table_catalog = True
                self._table_cash[row[0]].update_frequency = row[1]
                self._table_cash[row[0]].update_column = row[2]
                self._table_cash[row[0]].minimum_update_ts = row[3]
                self._table_cash[row[0]].next_regular_update_ts = row[4]
        except pyodbc.ProgrammingError as e:
            raise SqlValidatorError(f"Error when executing SQL: {str(e)}{chr(10)}{sql}")

    def _check_table_content(self) -> None:
        """For all tables found in the table_catalog, identify the latest timestamp in the upload column,
        and update the TableValidation objects accordingly."""
        cursor = self.db_connection.cursor()
        for tv in self.table_validations:
            if (
                tv.in_database is False
                and tv.table.created is False
                and tv.table.renamed is False
            ):
                tv.validation_error = f"table not found in the database"
                continue
            if not tv.in_table_catalog:
                tv.validation_error = f"table not registered in {self.table_catalog}"
                continue  # not in table catalog - skip item
            if not tv.update_column:
                tv.validation_error = f"update column name is blank"
                continue  # update column name blank - skip item
            if self._check_valid_characters(tv.update_column) is False:
                tv.validation_error = (
                    "the upload column name contains invalid characters"
                )
                continue  # invalid update column name - skip item
            if not tv.minimum_update_ts or not isinstance(
                tv.minimum_update_ts, datetime
            ):
                tv.validation_error = f"invalid minimum update timestamp in the table catalog:  {tv.minimum_update_ts}"
                continue  # invalid minimum update timestamp - skip item
            # check if table content is fresh (not applicable for created / renamed / populated tables)
            if (
                tv.table.created is False
                and tv.table.renamed is False
                and tv.table.populated is False
            ):
                sql = f"select max({tv.update_column}) from {tv.table.full_name}"
                try:
                    cursor.execute(sql)
                    row = cursor.fetchone()
                    if not row or row[0] is None:
                        tv.validation_error = "table is blank"
                        continue  # the table is blank, unable to identify the last update
                    if not isinstance(row[0], datetime):
                        tv.validation_error = (
                            f"invalid data type in {tv.update_column} - "
                        )
                        tv.validation_error += (
                            f"datetime expected, {type(row[0]).__name__} received"
                        )
                        continue  # can't interpret upload column - skip item
                    tv.actual_update_ts = row[0]
                except pyodbc.ProgrammingError:
                    tv.validation_error = f"unable to run query {sql}"
                    continue  # query execution failed - skip item
                if (
                    tv.minimum_update_ts > tv.actual_update_ts
                    and len(tv.table.parent_tables) == 0
                ):
                    tv.validation_error = f"outdated data - minimal update: {tv.minimum_update_ts}, actual update: {tv.actual_update_ts}"
                    continue  # source table with outdated content - skip item
                else:
                    tv.content_current = True
            tv.validation_successful = True

    @property
    def table_names_fully_qualified(self) -> list[str]:
        """Return a list of all fully qualified table names."""
        return list(self._table_cash.keys())

    @property
    def table_names_tuples(self) -> list[tuple[str]]:
        """Return a list of all table names in the format of tuples (tabschema, tabname)."""
        return [tuple(t.split(".")) for t in self.table_names_fully_qualified]

    @property
    def validation_successful(self) -> bool:
        """Boolean flag indicating if the validation was successful and the SQL should be executed.
        All the partial table validations have to be successful to return True."""
        if self.table_validations:
            validation = True
            for tv in self.table_validations:
                validation = validation and tv.validation_successful
        else:
            validation = False
        return validation


class SqlValidatorError(Exception):
    """Custom error for the SqlValidator class."""

    pass
