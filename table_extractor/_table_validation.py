from table_extractor._table import Table
from datetime import datetime
from dataclasses import dataclass


@dataclass
class TableValidation:
    """Extend Table class (storing DB general table characteristics) with the actual table content validation fields."""

    table: Table
    in_database: bool = False
    in_table_catalog: bool = False
    update_frequency: str = ""
    update_column: str = ""
    minimum_update_ts: datetime = None
    next_regular_update_ts: datetime = None
    actual_update_ts: datetime = None
    content_current: bool = False
    validation_successful: bool = False
    validation_error: str = ""


class TableValidationError(Exception):
    """Custom error for the TableValidation class."""

    pass
