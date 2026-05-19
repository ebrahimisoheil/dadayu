from __future__ import annotations

import dadayu.db
from dagster import ConfigurableResource


class ClickhouseResource(ConfigurableResource):
    def get_client(self):
        return dadayu.db.get_ch_client()
