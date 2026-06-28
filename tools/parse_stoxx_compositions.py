"""Reconstruct point-in-time DE index membership from the STOXX
"Historical Index Compositions of DAX Equity Indices" PDF.

Source (public, auto-updated):
https://www.stoxx.com/document/Indices/Common/Indexguide/Historical_Index_Compositions.pdf
"""
from __future__ import annotations

import csv
import re
import sys


def build_spans(events: list[dict], anchor: dict) -> list[dict]:
    """Turn an anchor composition + dated deletion/addition events into spans.

    A span is {index_name, company_name, valid_from, valid_to}. `valid_to=None`
    means still a member. Multiple non-contiguous spans per company are allowed.
    """
    index_name = anchor["index_name"]
    open_span: dict[str, dict] = {}
    spans: list[dict] = []

    def open_member(company: str, date: str) -> None:
        if company in open_span:
            return
        span = {"index_name": index_name, "company_name": company,
                "valid_from": date, "valid_to": None}
        open_span[company] = span
        spans.append(span)

    def close_member(company: str, date: str) -> None:
        span = open_span.pop(company, None)
        if span is not None:
            span["valid_to"] = date

    for company in anchor["companies"]:
        open_member(company, anchor["date"])

    for ev in sorted(events, key=lambda e: e["date_of_change"]):
        date = ev["date_of_change"]
        for company in _split_cell(ev.get("deletion", "")):
            close_member(company, date)
        for company in _split_cell(ev.get("addition", "")):
            open_member(company, date)

    return spans


def _split_cell(cell: str) -> list[str]:
    """One STOXX cell may list several companies on separate lines."""
    out = []
    for line in str(cell).splitlines():
        name = line.strip().rstrip("*").strip()  # '*' marks merged/renamed
        if name and name != "-":
            out.append(name)
    return out


# --- PDF extraction (best-effort; the committed CSV is source of truth) ---

# The PDF uses DD.MM.YYYY date format.
_DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")

# Map from the "Initial Composition" long-date text to index name.
# These are the opening anchors for each DE index section in the PDF
# (in the order the sections appear in the PDF).
_SECTION_ANCHORS = [
    ("30 December 1987", "DAX"),
    ("24 March 2003", "TecDAX"),
    ("11 April 1994", "MDAX"),
    ("21 June 1999", "SDAX"),
]

# The section after the 4 DE indices (DAX 50 ESG) marks the end of SDAX.
_SDAX_END_MARKER = "DAX® 50 ESG INDEX COMPOSITION"


def _iso(d: str) -> str:
    """Convert DD.MM.YYYY → YYYY-MM-DD."""
    dd, mm, yyyy = d.split(".")
    return f"{yyyy}-{mm}-{dd}"


def parse_compositions(pdf_path: str) -> list[dict]:
    """Extract anchor + events per index from the PDF, return all spans.

    This is a best-effort extractor. Review the output CSV against the PDF
    before committing; build_spans is the tested invariant.
    """
    from pypdf import PdfReader

    reader = PdfReader(pdf_path)
    full = "\n".join((p.extract_text() or "") for p in reader.pages)

    # Find section start positions using "Initial Composition" anchors.
    sections: list[tuple[int, str]] = []
    for long_date, index_name in _SECTION_ANCHORS:
        pattern = long_date + ": Initial Composition"
        pos = full.find(pattern)
        if pos != -1:
            sections.append((pos, index_name))
        else:
            print(f"  [WARN] anchor not found: {pattern!r}", file=sys.stderr)

    if not sections:
        print("  [ERROR] no sections found — PDF text extraction failed", file=sys.stderr)
        return []

    # Sort by position in the document.
    sections.sort(key=lambda x: x[0])

    # Find where SDAX ends (start of DAX 50 ESG section, first occurrence
    # that is not in the table of contents, i.e., well past position 1000).
    sdax_end_match = re.search(re.escape(_SDAX_END_MARKER), full[10000:])
    sdax_end = (10000 + sdax_end_match.start()) if sdax_end_match else len(full)

    spans: list[dict] = []
    for i, (start, index_name) in enumerate(sections):
        if i + 1 < len(sections):
            end = sections[i + 1][0]
        else:
            # Last section is SDAX; end before ESG indices.
            end = sdax_end

        section = full[start:end]
        anchor = _parse_anchor(section, index_name)
        events = _parse_events(section)
        print(
            f"  [{index_name}] anchor={anchor['date']} "
            f"companies={len(anchor['companies'])} events={len(events)}",
            file=sys.stderr,
        )
        spans.extend(build_spans(events, anchor))

    return spans


def _parse_anchor(section: str, index_name: str) -> dict:
    """Extract the initial-composition date and company list from a section."""
    m = re.search(r"(\d{1,2}\s+\w+\s+\d{4}):\s*Initial Composition", section)
    date = _iso_from_long(m.group(1)) if m else "1900-01-01"
    # Anchor companies appear between the Initial Composition header and the
    # first event date (DD.MM.YYYY) or the index name header line.
    head = section[m.end():] if m else section
    first_event = _DATE_RE.search(head)
    block = head[: first_event.start()] if first_event else head
    companies = [
        ln.strip()
        for ln in block.splitlines()
        if ln.strip()
        and "Date of" not in ln
        and not re.match(r"^[A-Z]+®?\s*$", ln.strip())  # skip bare index name lines
    ]
    return {"index_name": index_name, "date": date, "companies": companies}


def _parse_events(section: str) -> list[dict]:
    """Parse change-event rows from a section of extracted PDF text.

    Each row in the STOXX PDF looks like (DD.MM.YYYY format):
        18.09.1989  - -
    or (with deletion and addition in the same line):
        03.09.1990 22.05.1990 Feldmühle Nobel Metallgesellschaft
        Nixdorf * Preussag

    We scan for lines that start with a DD.MM.YYYY date and try to extract the
    deletion and addition names from the same logical row (which may span
    multiple text lines due to PDF text extraction quirks).
    """
    events: list[dict] = []
    lines = section.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m = re.match(r"^(\d{2}\.\d{2}\.\d{4})", line)
        if m:
            date_str = _iso(m.group(1))
            # Consume this line and possibly continuation lines for the same row.
            row_text = line
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                # Stop if the next line starts another date row.
                if re.match(r"^\d{2}\.\d{2}\.\d{4}", next_line):
                    break
                # Stop on section header-like lines.
                if re.match(r"^\d+\.\s+\w", next_line):
                    break
                # Stop on column header lines.
                if "Date of" in next_line or "HISTORICAL INDEX" in next_line:
                    break
                row_text += "\n" + next_line
                j += 1
            i = j
            deletion, addition = _parse_row_cells(row_text)
            events.append({
                "date_of_change": date_str,
                "deletion": deletion,
                "addition": addition,
            })
        else:
            i += 1
    return events


def _parse_row_cells(row_text: str) -> tuple[str, str]:
    """Split a raw row string into (deletion, addition) cell texts.

    The STOXX PDF table has columns:
        Date of change | Date of announcement | Deletion | Addition

    After stripping the two leading dates we try to split on the '*' separator
    that marks merger/rename boundaries in the raw extracted text.
    """
    # Remove leading dates (DD.MM.YYYY) — first one or two on the opening line.
    remainder = re.sub(r"^\d{2}\.\d{2}\.\d{4}\s*", "", row_text).strip()
    remainder = re.sub(r"^\d{2}\.\d{2}\.\d{4}\s*", "", remainder).strip()

    # A bare '-' / '- -' means no change on that side.
    if re.match(r"^-+\s*-*$", remainder.strip()):
        return "", ""
    # 'Conversion from …' rows are informational, not membership changes.
    if remainder.strip().startswith("Conversion"):
        return "", ""

    # Split on '*' which attaches to deleted company names (renames/mergers).
    # Strategy:
    #   • If '*' is directly attached to a word (e.g. "Kaufhof*"), the word
    #     before '*' (and everything before it on the same line) is the deletion;
    #     everything after is the addition.
    #   • If '*' has spaces around it (e.g. "Nixdorf * Preussag"), treat the
    #     text before '*' as deletion and after as addition.
    parts = re.split(r"\*", remainder, maxsplit=1)
    if len(parts) == 2:
        deletion = parts[0].strip().strip("-").strip()
        addition = parts[1].strip().strip("-").strip()
        return deletion, addition

    # No '*' found — try to split the two company names.
    text = remainder.strip()

    # Remove a leading '-' that signals "no deletion".
    if text.startswith("- "):
        return "", text[2:].strip()

    # Try splitting on 2+ consecutive spaces (column gap preserved in some rows).
    gap = re.search(r"  +", text)
    if gap:
        deletion = text[: gap.start()].strip().strip("-").strip()
        addition = text[gap.end():].strip().strip("-").strip()
        return deletion, addition

    # No clear separator: the text may contain one entry per line.
    # Treat each non-empty line as an addition (conservative; human review needed).
    lines = [ln.strip().strip("-").strip() for ln in text.splitlines() if ln.strip() and ln.strip() != "-"]
    if len(lines) == 1:
        # Single company — likely the deletion; addition unknown.
        return lines[0], ""
    if len(lines) == 2:
        # Two companies on two lines — best guess: first is deletion, second addition.
        return lines[0], lines[1]

    # Multiple continuation lines: treat all as additions (e.g. DAX 2021 expansion).
    return "", "\n".join(lines)


def _iso_from_long(s: str) -> str:
    import datetime as _dt

    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return _dt.datetime.strptime(s.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return "1900-01-01"


def _write_csv(spans: list[dict], out_path: str) -> None:
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["index_name", "company_name", "valid_from", "valid_to"])
        w.writeheader()
        for s in sorted(spans, key=lambda s: (s["index_name"], s["company_name"], s["valid_from"])):
            w.writerow({**s, "valid_to": s["valid_to"] or ""})


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 tools/parse_stoxx_compositions.py <pdf_path> <out_csv>", file=sys.stderr)
        sys.exit(1)
    pdf, out = sys.argv[1], sys.argv[2]
    spans = parse_compositions(pdf)
    _write_csv(spans, out)
    print(f"Wrote {len(spans)} rows to {out}")
    # Print first 10 rows for spot-check.
    import csv as _csv
    with open(out, newline="") as f:
        reader = _csv.DictReader(f)
        rows = list(reader)
    print(f"\nTotal rows (incl. header): {len(rows) + 1}  |  data rows: {len(rows)}")
    print("\nFirst 10 data rows:")
    for r in rows[:10]:
        print(r)
