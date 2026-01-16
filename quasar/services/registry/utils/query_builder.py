"""Dynamic SQL query building utilities."""

from typing import Any, List, Optional
from urllib.parse import unquote_plus


class FilterBuilder:
    """Build parameterized SQL WHERE clauses dynamically.

    Usage:
        builder = FilterBuilder()
        builder.add('class_name', params.class_name_like, partial_match=True)
        builder.add('class_type', params.class_type)
        builder.add('symbol', params.symbols, is_list=True)

        query = f"SELECT * FROM assets WHERE {builder.where_clause}"
        results = await conn.fetch(query, *builder.params)
    """

    def __init__(self, start_idx: int = 1):
        """Initialize FilterBuilder.

        Args:
            start_idx: Starting parameter index (default $1).
        """
        self.filters: List[str] = []
        self.params: List[Any] = []
        self._param_idx = start_idx

    def add(
        self,
        column: str,
        value: Optional[Any],
        partial_match: bool = False,
        is_list: bool = False,
    ) -> 'FilterBuilder':
        """Add a filter condition.

        Args:
            column: SQL column name.
            value: Filter value (None values are skipped).
            partial_match: Use LIKE with wildcards for partial matching.
            is_list: Parse comma-separated string as list for IN clause.

        Returns:
            Self for method chaining.
        """
        if value is None:
            return self

        if isinstance(value, str) and not value.strip():
            return self

        if isinstance(value, bool):
            self.filters.append(f"{column} = ${self._param_idx}")
            self.params.append(value)
            self._param_idx += 1
        elif is_list:
            decoded = unquote_plus(str(value).strip())
            list_values = [v.strip() for v in decoded.split(',') if v.strip()]
            if list_values:
                placeholders = ', '.join(
                    f'${self._param_idx + i}' for i in range(len(list_values))
                )
                self.filters.append(f"{column} IN ({placeholders})")
                self.params.extend(list_values)
                self._param_idx += len(list_values)
        elif partial_match:
            decoded = unquote_plus(str(value).strip())
            self.filters.append(f"LOWER({column}) LIKE LOWER(${self._param_idx})")
            self.params.append(f"%{decoded}%")
            self._param_idx += 1
        else:
            decoded = unquote_plus(str(value).strip()) if isinstance(value, str) else value
            self.filters.append(f"{column} = ${self._param_idx}")
            self.params.append(decoded)
            self._param_idx += 1

        return self

    @property
    def where_clause(self) -> str:
        """Get the WHERE clause string (without 'WHERE' keyword)."""
        return " AND ".join(self.filters) if self.filters else "TRUE"

    @property
    def next_param_idx(self) -> int:
        """Get the next available parameter index."""
        return self._param_idx
