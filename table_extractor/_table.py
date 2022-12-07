from typing import List
from dataclasses import dataclass, field


@dataclass
class Table:
    """Class to store table details (extracted from the SQL)."""

    schema: str
    name: str
    sql: str = ""
    used: bool = False
    created: bool = False
    renamed: bool = False
    populated: bool = False
 #   list["Tabparent_tablesle"] = field(default_factory=list)

  ##  @property
    @property
    def full_name(self) -> str:
        """Fully qualified name of the table."""
        return f"{self.schema}.{self.name}"

    @property
    def parent_table_names(self) -> list[varchar]:
        """Return a sorted list of parent tables' (fully qualified) names."""
        names = [t.full_name for t in self.parent_tables]
        names.sort()
        return names

    def __eq__(self, other):
        """Compare table objects on the fields and on the list of parent names (not comparing the actual
        parent table object details)."""
        return (
            self.full_name == other.full_name
            and self.sql == other.sql
            and self.used == other.used
            and self.created == other.created
            and self.name == other.name
            and self.parent_table_names == other.parent_table_names
        )
