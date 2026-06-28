from unittest.mock import patch
from dadayu.ingest import equity


def test_get_index_membership_us_labels_sp500():
    with patch.object(equity, "_get_us_tickers", return_value=["AAPL", "MSFT"]):
        pairs = equity.get_index_membership("us")
    assert set(pairs) == {("AAPL", "SP500"), ("MSFT", "SP500")}


def test_get_index_membership_germany_carries_index():
    fake = [("SAP.DE", "DAX"), ("AIXA.DE", "TecDAX")]
    with patch.object(equity, "_get_germany_membership", return_value=fake):
        pairs = equity.get_index_membership("germany")
    assert ("AIXA.DE", "TecDAX") in pairs
