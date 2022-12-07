import pyodbc

from table_extractor._table import Table
from table_extractor._table_extractor import TableExtractor, TableExtractorError
from table_extractor._table_validation import TableValidation, TableValidationError
from table_extractor._sql_validator import SqlValidator, SqlValidatorError


# GitHub login name
user_name = "Gagan.Gupta@kyndryl.com"
# GitHub personnal access token
access_token = "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"
# file URL (both standard WEB link and the RAW link are accepted)
url = "https://raw.github.kyndryl.net/etl-chapter/table_extractor/master/tests/resources/sysdummy1.sql"

te = TableExtractor()
te.from_github(url, user_name, access_token)
#print('Github :-',te)

# run SQL analyzes
te.analyze()

# to generate a pandas DataFrame with the tables
df = te.tables_to_data_frame()
print('DF :-',df)

