from dataclasses import dataclass, field
from table_extractor._table import Table
import re
import pandas as pd
import requests
import io


@dataclass
class TableExtractor:
    """Extract table list from a SQL statement.

    The SQL statement can be provided by different methods:
    - as a string when creating the object, e.g. TableExtractor(sql='select 1 from sysibm.sysdummy1')
    - from a file for an existing object
    - from a file stored on GitHub

    To analyze the SQL statement, call the analyze() method. The result (list of Table objects) is accessible
    in the 'tables' field.
    """

    sql: str = ""
    file_name: str = ""
    url: str = ""
    tables: list[Table] = field(default_factory=list)
    _table_cash: dict[str:Table] = field(default_factory=dict)
    _sql_statements: list[str] = field(default_factory=list)
    sql_clean = ""

    def from_file(self, file_name: str) -> None:
        """Load SQL statement from a file."""
        try:
            with open(file_name) as f:
                self.sql = f.read()
                self.file_name = file_name
        except FileNotFoundError:
            raise TableExtractorError(f"File {file_name} not found!")

    def from_github(self, file_url: str, user_name: str, access_token: str) -> None:
        """Load SQL statement from a GitHub repository:
        - file_url: URL of the GitHub file in the RAW or in the WEB format
        - user_name: required for authentication to the private repositories
        - access_token: a personal access token required for authentication to the private repositories"""
        file_url = self._clean_github_url(file_url)
        try:
            github_session = requests.Session()
            github_session.auth = (user_name, access_token)
            download = github_session.get(file_url).content
        except requests.RequestException as e:
            raise TableExtractorError(
                f"""Unable to download the file. Pls check the provided file_url and user_name / 
               access token. {e}"""
            )
        self.sql = io.StringIO(download.decode("utf-8")).read()
        self.url = file_url

    def analyze(self) -> None:
        """Analyze the SQL statement to identify referenced tables."""
        # remove comments, blank lines and indentation
        self.sql_clean = self._remove_comments(self.sql)
        self.sql_clean = self._remove_blank_lines(self.sql_clean)
        self.sql_clean = self._trim_lines(self.sql_clean)
        # parse to the individual statements
        self._sql_statements = self.sql_clean.split(";")
        # loop through the statements
        for sql in self._sql_statements:
            source_tables = self._identify_source_tables(sql)
            target_tables = self._identify_target_tables(sql)
            renamed_tables = self._identify_renamed_tables(sql)
            populated_tables = self._identify_populated_tables(sql)
            # identify source tables
            for st in source_tables:
                if st in self._table_cash.keys():
                    self._table_cash[st].used = True
                else:
                    self._table_cash[st] = Table(
                        schema=st.split(".")[0], name=st.split(".")[1], used=True
                    )
            # identify target tables
            for tt in target_tables:
                if tt not in self._table_cash.keys():
                    self._table_cash[tt] = Table(
                        schema=tt.split(".")[0], name=tt.split(".")[1]
                    )
                self._table_cash[tt].created = True
                self._table_cash[tt].sql = sql
                # link parent tables
                #for st in source_tables:
                #    if st not in self._table_cash[tt].parent_table_names:
                #        self._table_cash[tt].parent_tables.append(self._table_cash[st])
            # identify renamed tables
            for rt in renamed_tables:
                original_table = rt[0]
                renamed_table = rt[1]
                # update original_table in the cash
                if original_table not in self._table_cash.keys():
                    self._table_cash[original_table] = Table(
                        schema=original_table.split(".")[0],
                        name=original_table.split(".")[1],
                    )
                self._table_cash[original_table].used = True
                # update renamed table in the cash
                if renamed_table not in self._table_cash.keys():
                    self._table_cash[renamed_table] = Table(
                        schema=renamed_table.split(".")[0],
                        name=renamed_table.split(".")[1],
                    )
                self._table_cash[renamed_table].renamed = True
                self._table_cash[renamed_table].sql = sql
                # link original nad renamed tables together
                #if (
                #    original_table
                #    not in self._table_cash[renamed_table].parent_table_names
                # ):
                #    self._table_cash[renamed_table].parent_tables.append(
                #        self._table_cash[original_table]
                #    )
            # identify populated tables
            for pt in populated_tables:
                if pt in self._table_cash.keys():
                    self._table_cash[pt].populated = True
                else:
                    self._table_cash[pt] = Table(
                        schema=pt.split(".")[0], name=pt.split(".")[1], populated=True
                    )
                self._table_cash[pt].sql = sql
                # link parent tables
            #    for st in source_tables:
            #        if st not in self._table_cash[pt].parent_table_names:
            #            self._table_cash[pt].parent_tables.append(self._table_cash[st])
            # update tables list
            self.tables = [t for t in self._table_cash.values()]

    def tables_to_data_frame(self) -> pd.DataFrame:
        """Export table details in a DataFrame format."""
        data = []
        table_cash = {}
        # map table names to their future indexes in the dataframe
        for idx, t in enumerate(self.tables):
            table_cash[t.full_name] = idx
        # loop through the tables
        for t in self.tables:
            # build a list of parent table indexes
            # parent_tables = []
            # for pt in t.parent_tables:
            #    parent_tables.append(table_cash[pt.full_name])
            # add data row
            data.append(
                [
                    t.schema,
                    t.name,
                    t.full_name,
                    t.used,
                    t.created,
                    t.renamed,
                    t.populated,
                    t.sql,
                    t.name,
                    # parent_tables,
                ]
            )
        # create the dataframe
        index = range(len(self.tables))
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
        df = pd.DataFrame(data, columns=columns, index=index)
        return df

    def tables_to_edge_data_frame(self) -> pd.DataFrame:
        """Export tables and their relationship in the edge list format for networkx plot creation"""
        data = []
        # loop through the tables
        for t in self.tables:
            # for each table loop through the parent tables
            for pt in t.parent_tables:
                data.append([pt.full_name, t.full_name])
        # create the dataframe
        columns = ["SOURCE", "TARGET"]
        df = pd.DataFrame(data, columns=columns)
        return df

    def tables_relationship_chart(self):
        """Plot a table relationship diagram."""
        # try to import plotting libraries
        try:
            import networkx as nx
        except ModuleNotFoundError:
            raise TableExtractorError(
                "Can't import networkx module. Pls install networkx and try again."
            )
        try:
            import matplotlib.pyplot as plt
        except ModuleNotFoundError:
            raise TableExtractorError(
                "Can't import matplotlib module. Pls install matplotlib and try again."
            )
        # get source data
        edge_df = self.tables_to_edge_data_frame()
        # break the long table name into 2 lines
        edge_df.SOURCE = edge_df.SOURCE.str.replace(".", "\n", regex=False)
        edge_df.TARGET = edge_df.TARGET.str.replace(".", "\n", regex=False)
        graph = nx.from_pandas_edgelist(
            edge_df,
            source="SOURCE",
            target="TARGET",
            create_using=nx.MultiDiGraph(),
        )
        options = {
            "node_size": 30,
            "alpha": 0.8,
            "node_color": "blue",
            "node_shape": "s",
        }
        nx.draw_networkx(graph, **options)
        # Set margins for the axes so that nodes aren't clipped
        ax = plt.gca()
        ax.margins(0.20)
        plt.axis("off")
        plt.show()

    @staticmethod
    def _remove_blank_lines(sql: str) -> str:
        """Remove lines which are empty or which contain whitespaces only."""
        sql = re.sub("(^\s*\r\n$)", "", sql)  # CR+LF
        sql = re.sub("(^\s*\n$)", "", sql)  # LF
        return sql

    @staticmethod
    def _trim_lines(sql: str) -> str:
        """Removing all leading and closing white spaces for each line."""
        lines = sql.split("\n")
        lines_trimmed = [line.strip() for line in lines]
        return "\n".join(lines_trimmed)

    @staticmethod
    def _remove_comments(sql: str) -> str:
        """Remove comments from the provided SQL statement:
        - comments with leading double-dash - remove the rest of the line
        - multi-line comments marked by slash + star - remove all text in between the markers"""
        # remove single-line comments
        lines = sql.split("\n")
        lines_updated = [re.sub("(--.*$)", "", line) for line in lines]
        sql = "\n".join(lines_updated)
        # remove multi-line comments (any character between /* and */
        sql = re.sub(r"/\*[^*/]*\*/", "", sql)
        return sql

    @staticmethod
    def _identify_source_tables(sql: str) -> set[str]:
        """Search in the provided SQL statement for source tables (in FROM / JOIN clauses). Return a set
        of fully qualified table names (tabschema.tabname) converted to upper case."""
        # remove EXTRACT FROM TABLE.COLUMN / TRIM FROM TABLE.COLUMN snippets, so the they are not falsely
        # considered to be source tables
        sql = re.sub("trim\s*\([^)]*from[^)]*\)", "x", sql, flags=re.IGNORECASE)
        sql = re.sub("extract\s*\([^)]*from[^)]*\)", "x", sql, flags=re.IGNORECASE)
        # identify source tables
        from_tables = re.findall(
            "from\s+([a-z\d_\-]+)\s*\.\s*([a-z\d_\-]+)", sql, flags=re.IGNORECASE
        )
        joined_tables = re.findall(
            "join\s+([a-z\d_\-]+)\s*\.\s*([a-z\d_\-]+)", sql, flags=re.IGNORECASE
        )
        result = from_tables + joined_tables
        tables = set()
        for r in result:
            table = f"{r[0].upper()}.{r[1].upper()}"
            if table not in tables:
                tables.add(table)
        return tables

    @staticmethod
    def _identify_target_tables(sql: str) -> set[str]:
        """Search in the provided SQL statement for target tables (in CREATE TABLE / CREATE HADOOP TABLE clauses).
        Return a set of fully qualified table names (tabschema.tabname) converted to upper case."""
        create_table = re.findall(
            "create\s+table\s+([a-z\d_\-]+)\s*\.\s*([a-z\d_\-]+)",
            sql,
            flags=re.IGNORECASE,
        )
        create_hadoop_table = re.findall(
            "create\s+hadoop\s+table\s+([a-z\d_\-]+)\s*\.\s*([a-z\d_\-]+)",
            sql,
            flags=re.IGNORECASE,
        )
        result = create_table + create_hadoop_table
        tables = set()
        for r in result:
            table = f"{r[0].upper()}.{r[1].upper()}"
            if table not in tables:
                tables.add(table)
        return tables

    @staticmethod
    def _identify_renamed_tables(sql: str) -> set[tuple[str, str]]:
        """Search in the provided SQL statement for renamed tables (in FROM / JOIN clauses). Return a set
        of tuples, each of them consisting of the fully qualified original table name and new table name.
        The table names are converted to upper case."""
        result = re.findall(
            "rename\s+table\s+([a-z\d_\-]+)\s*\.\s*([a-z\d_\-]+)\s+to\s+([a-z\d_\-]+)",
            sql,
            flags=re.IGNORECASE,
        )
        tables = set()
        for r in result:
            original_name = f"{r[0].upper()}.{r[1].upper()}"
            new_name = f"{r[0].upper()}.{r[2].upper()}"
            table = (original_name, new_name)
            if table not in tables:
                tables.add(table)
        return tables

    @staticmethod
    def _identify_populated_tables(sql: str) -> set[str]:
        """Search in the provided SQL statement for tables which are being populated  - either with
        INSERT INTO, or with (LOAD HADOOP xxx) INTO TABLE."""
        result_into_table = re.findall(
            "into\s+table\s+([a-z\d_\-]+)\s*\.\s*([a-z\d_\-]+)",
            sql,
            flags=re.IGNORECASE,
        )
        result_insert_into = re.findall(
            "insert\s+into\s+([a-z\d_\-]+)\s*\.\s*([a-z\d_\-]+)",
            sql,
            flags=re.IGNORECASE,
        )
        result = result_into_table + result_insert_into
        tables = set()
        for r in result:
            table = f"{r[0].upper()}.{r[1].upper()}"
            if table not in tables:
                tables.add(table)
        return tables

    @staticmethod
    def _clean_github_url(url: str) -> str:
        """Check if the GitHub URL have been provided in a 'raw' format:
        - if it is, remove token parameter if present ('?token=....')
        - if not, convert url to the raw format (add 'raw' subdomain, remove 'blob' folder"""
        if "//raw.github." in url:
            parsed_url = url.split("?")[0]
        elif "//github." in url:
            parsed_url = url.replace("//github.", "//raw.github.").replace(
                "/blob/", "/"
            )
        else:
            parsed_url = url
        return parsed_url


class TableExtractorError(Exception):
    """Custom error for the TableExtractor class."""

    pass
