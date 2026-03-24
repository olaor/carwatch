"""
finnwatch_tui.py – Textual TUI for browsing finnwatch.db.

One tab per ad_type found in the database.  Car ads show sortable columns
for make, model, year, KM, price, Δ price, and first-seen date.
Other ad types show a generic set of columns.

Key bindings:
    q / Ctrl-C      quit
    r               refresh data from DB  (resets sort)
    i               toggle inactive (removed) ads
    o / Enter       open selected ad in browser

Dependencies:
    pip install textual
"""

from __future__ import annotations

import argparse
import sqlite3
import webbrowser
from pathlib import Path
from typing import Optional

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Header, Label, TabbedContent, TabPane

# ─────────────────────────────────────────────────────────────────────────────
#  Paths
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = Path.home() / ".finnwatch" / "database" / "finnwatch.db"

# ─────────────────────────────────────────────────────────────────────────────
#  SQL
# ─────────────────────────────────────────────────────────────────────────────

# Car query: pull make/model/year/mileage from the raw_data JSON column.
# Model is derived by stripping the make from the title (e.g. "BMW 3-serie" → "3-serie").
# Price diff = latest snapshot price minus first snapshot price.
CAR_QUERY = """
WITH first_snap AS (
    SELECT finnkode, price
    FROM   ad_snapshots
    WHERE  (finnkode, snapshot_time) IN (
               SELECT finnkode, MIN(snapshot_time)
               FROM   ad_snapshots
               GROUP  BY finnkode
           )
),
last_snap AS (
    SELECT finnkode, price, raw_data, title
    FROM   ad_snapshots
    WHERE  (finnkode, snapshot_time) IN (
               SELECT finnkode, MAX(snapshot_time)
               FROM   ad_snapshots
               GROUP  BY finnkode
           )
)
SELECT
    a.finnkode,
    COALESCE(json_extract(ls.raw_data, '$.make'), '')  AS make,
    TRIM(REPLACE(
        COALESCE(ls.title, ''),
        COALESCE(json_extract(ls.raw_data, '$.make'), '') || ' ',
        ''
    ))                                                 AS model,
    COALESCE(CAST(json_extract(ls.raw_data, '$.year')    AS INTEGER), 0) AS year,
    COALESCE(CAST(json_extract(ls.raw_data, '$.mileage') AS INTEGER), 0) AS mileage,
    COALESCE(ls.price, 0)                              AS current_price,
    COALESCE(ls.price - fs.price, 0)                   AS price_diff,
    SUBSTR(a.first_seen, 1, 10)                        AS first_seen,
    a.is_active,
    a.url
FROM   ads a
LEFT   JOIN first_snap fs ON fs.finnkode = a.finnkode
LEFT   JOIN last_snap  ls ON ls.finnkode = a.finnkode
WHERE  a.ad_type = 'car'
"""

# Generic query for any other ad type.
GENERIC_QUERY = """
WITH last_snap AS (
    SELECT finnkode, price, title, location, seller_name
    FROM   ad_snapshots
    WHERE  (finnkode, snapshot_time) IN (
               SELECT finnkode, MAX(snapshot_time)
               FROM   ad_snapshots
               GROUP  BY finnkode
           )
)
SELECT
    a.finnkode,
    COALESCE(ls.title,       '') AS title,
    COALESCE(ls.price,        0) AS price,
    COALESCE(ls.location,    '') AS location,
    COALESCE(ls.seller_name, '') AS seller,
    SUBSTR(a.first_seen, 1, 10)  AS first_seen,
    a.is_active,
    a.url
FROM   ads a
LEFT   JOIN last_snap ls ON ls.finnkode = a.finnkode
WHERE  a.ad_type = ?
"""

# ─────────────────────────────────────────────────────────────────────────────
#  Column specs: list of (key, display_label, width)
# ─────────────────────────────────────────────────────────────────────────────

CAR_COLS: list[tuple] = [
    ("make",          "Make",       14),
    ("model",         "Model",      20),
    ("year",          "Year",        6),
    ("mileage",       "KM",         10),
    ("current_price", "Price",      13),
    ("price_diff",    "Δ Price",    11),
    ("first_seen",    "First seen", 12),
    ("finnkode",      "Finnkode",   12),
]

GENERIC_COLS: list[tuple] = [
    ("title",      "Title",       48),
    ("price",      "Price",       13),
    ("location",   "Location",    18),
    ("seller",     "Seller",      22),
    ("first_seen", "First seen",  12),
    ("finnkode",   "Finnkode",    12),
]

# ─────────────────────────────────────────────────────────────────────────────
#  Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

_THIN = "\u202f"   # NARROW NO-BREAK SPACE  (thousands separator)
_NBSP = "\u00a0"   # NO-BREAK SPACE          (before unit)


def _fmt(n: int, unit: str) -> str:
    if not n:
        return ""
    return f"{n:,}".replace(",", _THIN) + _NBSP + unit


fmt_kr = lambda n: _fmt(n, "kr")
fmt_km = lambda n: _fmt(n, "km")


def price_diff_cell(diff: int) -> Text:
    if not diff:
        return Text("—", style="dim")
    sign  = "+" if diff > 0 else ""
    style = "bold red" if diff > 0 else "bold green"
    s     = f"{diff:,}".replace(",", _THIN)
    return Text(f"{sign}{s}", style=style)


# ─────────────────────────────────────────────────────────────────────────────
#  Sorting helper
# ─────────────────────────────────────────────────────────────────────────────

def _key(row: sqlite3.Row, col: str):
    """Comparable sort key for a sqlite3.Row, with NULLs last."""
    try:
        val = row[col]
    except Exception:
        return (1, "")
    if val is None:
        return (1, "")
    if isinstance(val, str):
        return (0, val.lower())
    return (0, val)


def sort_rows(rows: list, col: str, reverse: bool) -> list:
    return sorted(rows, key=lambda r: _key(r, col), reverse=reverse)


# ─────────────────────────────────────────────────────────────────────────────
#  App
# ─────────────────────────────────────────────────────────────────────────────

class FinnWatchTUI(App):
    """Browse finn.no ads stored in finnwatch.db."""

    TITLE = "FinnWatch"

    CSS = """
    DataTable {
        height: 1fr;
    }
    TabPane {
        padding: 0;
        height: 1fr;
    }
    #msg {
        margin: 1 2;
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("q",     "quit",            "Quit"),
        Binding("r",     "refresh",         "Refresh"),
        Binding("o",     "open_ad",         "Open in browser"),
        Binding("enter", "open_ad",         "Open in browser", show=False),
        Binding("i",     "toggle_inactive", "Toggle inactive"),
        Binding("left",  "sort_prev_col",   "Sort ◀"),
        Binding("right", "sort_next_col",   "Sort ▶"),
        Binding("s",     "sort_reverse",    "Flip sort"),
    ]

    def __init__(self, db_path: Path) -> None:
        super().__init__()
        self.db_path        = db_path
        self._show_inactive = False
        self._rows:  dict[str, list]  = {}   # ad_type → list[sqlite3.Row]
        self._urls:  dict[str, str]   = {}   # finnkode → url
        self._sort:  dict[str, tuple] = {}   # ad_type → (col_key, reverse)
        self._types: list[str]        = []

    # ── Layout ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield TabbedContent(id="tabs")
        yield Footer()

    async def on_mount(self) -> None:
        self.sub_title = str(self.db_path)
        if not self.db_path.exists():
            tc = self.query_one("#tabs", TabbedContent)
            await tc.add_pane(
                TabPane("!", Label(f"Database not found: {self.db_path}", id="msg"))
            )
            return
        await self._rebuild()

    # ── Data loading ──────────────────────────────────────────────────────────

    def _load(self) -> None:
        """Fetch all rows from DB into self._rows / self._urls."""
        db = sqlite3.connect(str(self.db_path))
        db.row_factory = sqlite3.Row
        self._types = [
            r[0]
            for r in db.execute(
                "SELECT ad_type, COUNT(*) c FROM ads GROUP BY ad_type ORDER BY c DESC"
            ).fetchall()
        ]
        for ad_type in self._types:
            rows = (
                db.execute(CAR_QUERY).fetchall()
                if ad_type == "car"
                else db.execute(GENERIC_QUERY, (ad_type,)).fetchall()
            )
            self._rows[ad_type] = rows
            for r in rows:
                if r["url"]:
                    self._urls[str(r["finnkode"])] = str(r["url"])
        db.close()

    # ── Tab/table building ────────────────────────────────────────────────────

    async def _rebuild(self) -> None:
        """Reload from DB and recreate all tabs."""
        self._load()
        tc = self.query_one("#tabs", TabbedContent)
        active_before = str(tc.active or "")

        for pane in list(tc.query(TabPane)):
            await tc.remove_pane(pane.id)

        if not self._types:
            await tc.add_pane(
                TabPane("(empty)", Label("No ads in database yet.", id="msg"))
            )
            return

        for ad_type in self._types:
            rows  = self._rows[ad_type]
            count = sum(1 for r in rows if self._show_inactive or r["is_active"])
            table = DataTable(
                id=f"tbl_{ad_type}",
                cursor_type="row",
                zebra_stripes=True,
            )
            await tc.add_pane(
                TabPane(f"{ad_type.title()} ({count})", table, id=f"tab_{ad_type}")
            )
            self._fill(table, ad_type)

        # Restore previously active tab if it still exists
        valid = {f"tab_{t}" for t in self._types}
        if active_before in valid:
            try:
                tc.active = active_before
            except Exception:
                pass

    def _fill(self, table: DataTable, ad_type: str) -> None:
        """Clear and repopulate *table* with (optionally sorted) rows."""
        table.clear(columns=True)
        cols               = CAR_COLS if ad_type == "car" else GENERIC_COLS
        sort_col, sort_rev = self._sort.get(ad_type, (None, False))
        rows               = self._rows.get(ad_type, [])

        if sort_col:
            rows = sort_rows(rows, sort_col, sort_rev)

        # Add columns, appending a sort direction arrow to the active column.
        for key, label, width in cols:
            arrow = (" ▼" if sort_rev else " ▲") if key == sort_col else ""
            table.add_column(label + arrow, key=key, width=width)

        # Add rows, skipping inactive ads when the toggle is off.
        for row in rows:
            if not self._show_inactive and not row["is_active"]:
                continue
            table.add_row(*self._cells(row, ad_type), key=str(row["finnkode"]))

    def _cells(self, row: sqlite3.Row, ad_type: str) -> list:
        if ad_type == "car":
            return [
                str(row["make"]  or ""),
                str(row["model"] or ""),
                str(row["year"]  or ""),
                fmt_km(row["mileage"]),
                fmt_kr(row["current_price"]),
                price_diff_cell(row["price_diff"] or 0),
                str(row["first_seen"] or ""),
                str(row["finnkode"]),
            ]
        return [
            str(row["title"]    or ""),
            fmt_kr(row["price"]),
            str(row["location"] or ""),
            str(row["seller"]   or ""),
            str(row["first_seen"] or ""),
            str(row["finnkode"]),
        ]

    # ── Events ────────────────────────────────────────────────────────────────

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        """Toggle sort on mouse-clicked column."""
        col     = str(event.column_key)
        ad_type = self._active_type()
        if not ad_type:
            return
        prev_col, prev_rev = self._sort.get(ad_type, (None, False))
        self._sort[ad_type] = (col, (not prev_rev) if col == prev_col else False)
        self._fill(event.data_table, ad_type)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _active_type(self) -> Optional[str]:
        tc      = self.query_one("#tabs", TabbedContent)
        pane_id = str(tc.active or "")
        return pane_id[4:] if pane_id.startswith("tab_") else None

    def _active_table_and_type(self) -> tuple[Optional[DataTable], Optional[str]]:
        ad_type = self._active_type()
        if not ad_type:
            return None, None
        try:
            return self.query_one(f"#tbl_{ad_type}", DataTable), ad_type
        except Exception:
            return None, None

    def _cols_for(self, ad_type: str) -> list[tuple]:
        return CAR_COLS if ad_type == "car" else GENERIC_COLS

    def _cursor_finnkode(self, table: DataTable, ad_type: str) -> Optional[str]:
        if table.row_count == 0:
            return None
        try:
            row = table.get_row_at(table.cursor_row)
        except Exception:
            return None
        cols   = self._cols_for(ad_type)
        fk_idx = next((i for i, (k, *_) in enumerate(cols) if k == "finnkode"), -1)
        if fk_idx < 0 or fk_idx >= len(row):
            return None
        return str(row[fk_idx])

    # ── Actions ───────────────────────────────────────────────────────────────

    async def action_refresh(self) -> None:
        self._sort.clear()
        await self._rebuild()
        self.notify("Refreshed from database")

    async def action_toggle_inactive(self) -> None:
        self._show_inactive = not self._show_inactive
        await self._rebuild()
        self.notify("Inactive ads " + ("shown" if self._show_inactive else "hidden"))

    def action_sort_next_col(self) -> None:
        self._step_sort_col(+1)

    def action_sort_prev_col(self) -> None:
        self._step_sort_col(-1)

    def action_sort_reverse(self) -> None:
        table, ad_type = self._active_table_and_type()
        if not ad_type or table is None:
            return
        col, rev = self._sort.get(ad_type, (self._cols_for(ad_type)[0][0], False))
        self._sort[ad_type] = (col, not rev)
        self._fill(table, ad_type)

    def _step_sort_col(self, delta: int) -> None:
        table, ad_type = self._active_table_and_type()
        if not ad_type or table is None:
            return
        cols              = self._cols_for(ad_type)
        keys              = [k for k, *_ in cols]
        cur_col, cur_rev  = self._sort.get(ad_type, (keys[0], False))
        idx               = keys.index(cur_col) if cur_col in keys else 0
        new_col           = keys[(idx + delta) % len(keys)]
        # Stepping to a new column always starts ascending
        self._sort[ad_type] = (new_col, False)
        self._fill(table, ad_type)
        self.notify(f"Sort: {new_col}  ▲")

    def action_open_ad(self) -> None:
        ad_type = self._active_type()
        if not ad_type:
            return
        try:
            table = self.query_one(f"#tbl_{ad_type}", DataTable)
        except Exception:
            return
        finnkode = self._cursor_finnkode(table, ad_type)
        if not finnkode:
            return
        url = self._urls.get(finnkode) or (
            f"https://www.finn.no/mobility/item/{finnkode}"
            if ad_type == "car"
            else f"https://www.finn.no/ad/{finnkode}"
        )
        webbrowser.open(url)
        self.notify(f"Opening {finnkode} …")


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="FinnWatch TUI – browse tracked finn.no ads"
    )
    ap.add_argument(
        "--db",
        default=str(DB_PATH),
        metavar="PATH",
        help="Path to finnwatch.db  (default: %(default)s)",
    )
    args = ap.parse_args()
    FinnWatchTUI(db_path=Path(args.db)).run()
