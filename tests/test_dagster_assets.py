from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from dagster_pipeline.resources import ClickhouseResource


def test_clickhouse_resource_calls_get_ch_client():
    with patch("dadayu.db.get_ch_client") as mock_get:
        mock_get.return_value = MagicMock()
        resource = ClickhouseResource()
        client = resource.get_client()
    mock_get.assert_called_once()
    assert client is mock_get.return_value
