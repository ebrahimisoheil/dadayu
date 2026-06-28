from tools.parse_stoxx_compositions import build_spans


def test_build_spans_open_close_reopen():
    anchor = {"date": "2003-03-24", "companies": ["AIXTRON", "Jenoptik"], "index_name": "TecDAX"}
    events = [
        {"date_of_change": "2005-09-19", "deletion": "AIXTRON", "addition": "Infineon"},
        {"date_of_change": "2009-09-21", "deletion": "Infineon", "addition": "AIXTRON"},
    ]
    spans = build_spans(events, anchor)
    # AIXTRON: in from anchor, out 2005-09-19, back in 2009-09-21 (open)
    aix = sorted([s for s in spans if s["company_name"] == "AIXTRON"], key=lambda s: s["valid_from"])
    assert aix[0] == {"index_name": "TecDAX", "company_name": "AIXTRON",
                      "valid_from": "2003-03-24", "valid_to": "2005-09-19"}
    assert aix[1] == {"index_name": "TecDAX", "company_name": "AIXTRON",
                      "valid_from": "2009-09-21", "valid_to": None}
    # Infineon: in 2005-09-19, out 2009-09-21
    inf = [s for s in spans if s["company_name"] == "Infineon"]
    assert inf == [{"index_name": "TecDAX", "company_name": "Infineon",
                    "valid_from": "2005-09-19", "valid_to": "2009-09-21"}]
    # Jenoptik: anchor, never removed -> open span
    jen = [s for s in spans if s["company_name"] == "Jenoptik"]
    assert jen == [{"index_name": "TecDAX", "company_name": "Jenoptik",
                    "valid_from": "2003-03-24", "valid_to": None}]
