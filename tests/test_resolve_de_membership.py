from tools.resolve_de_membership import resolve


def test_resolve_maps_known_names_and_reports_unmapped():
    rows = [
        {"index_name": "DAX", "company_name": "Infineon", "valid_from": "2009-03-23", "valid_to": ""},
        {"index_name": "DAX", "company_name": "Daimler", "valid_from": "2010-01-01", "valid_to": "2021-02-01"},
        {"index_name": "DAX", "company_name": "Defunct AG", "valid_from": "2011-01-01", "valid_to": ""},
    ]
    crosswalk = {"Infineon": "IFX.DE", "Daimler": "MBG.DE"}
    resolved, unmapped = resolve(rows, crosswalk)
    assert {"ticker": "IFX.DE", "market": "germany", "index_name": "DAX",
            "valid_from": "2009-03-23", "valid_to": None} in resolved
    assert {"ticker": "MBG.DE", "market": "germany", "index_name": "DAX",
            "valid_from": "2010-01-01", "valid_to": "2021-02-01"} in resolved
    assert unmapped == ["Defunct AG"]
