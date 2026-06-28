"""Dagster asset modules.

Keep this package initializer lightweight so importing one asset module does not
eagerly construct every dbt-backed asset definition.
"""

__all__: list[str] = []
