import sqlite3
import json
import gzip
from pathlib import Path

DB_PATH = "etymology.db"
JSONL_GZ_PATH = "filtered-etymology.jsonl.gz"  # adjust if needed

SCHEMA_ENTRIES = """
CREATE TABLE IF NOT EXISTS entries (
    -- synthetic key for graph + joins
    node_id           TEXT PRIMARY KEY,

    -- your target_keys
    word              TEXT NOT NULL,
    lang              TEXT,
    lang_code         TEXT NOT NULL,
    pos               TEXT,
    etymology_number  INTEGER,
    etymology_text    TEXT,
    etymology_templates TEXT,  -- store JSON string
    derived           TEXT,    -- JSON
    descendants       TEXT,    -- JSON
    alt_of            TEXT,    -- JSON
    form_of           TEXT,    -- JSON
    categories        TEXT,    -- JSON (you can filter later)
    redirects         TEXT,    -- JSON
    literal_meaning   TEXT,
    wikidata          TEXT
);

CREATE INDEX IF NOT EXISTS idx_entries_lang_word_pos
    ON entries(lang_code, word, pos);

CREATE INDEX IF NOT EXISTS idx_entries_word
    ON entries(word);
"""

SCHEMA_EDGES = """
CREATE TABLE IF NOT EXISTS edges (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    src_id        TEXT NOT NULL,
    dst_id        TEXT NOT NULL,
    relation_type TEXT NOT NULL,  -- e.g. 'inh', 'der', 'bor', 'alt_of', 'form_of'
    raw_payload   TEXT,           -- optional JSON with original template/args

    FOREIGN KEY (src_id) REFERENCES entries(node_id),
    FOREIGN KEY (dst_id) REFERENCES entries(node_id)
);

CREATE INDEX IF NOT EXISTS idx_edges_src
    ON edges(src_id);

CREATE INDEX IF NOT EXISTS idx_edges_dst
    ON edges(dst_id);

CREATE INDEX IF NOT EXISTS idx_edges_type
    ON edges(relation_type);
"""


def init_db(path: str = DB_PATH) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA_ENTRIES)
    conn.executescript(SCHEMA_EDGES)
    conn.commit()
    conn.close()


def make_node_id(entry: dict) -> str:
    """
    Canonical node ID: lang_code:word:pos:etymology_number
    etymology_number defaults to 0 if missing.
    pos can be None/empty; that's fine.
    """
    lang_code = entry.get("lang_code") or ""
    word = entry.get("word") or ""
    pos = entry.get("pos") or ""
    ety_num = entry.get("etymology_number")
    if ety_num is None:
        ety_num = 0
    return f"{lang_code}:{word}:{pos}:{ety_num}"


# fields that should be JSON-encoded before storage
JSON_FIELDS = {
    "etymology_templates",
    "derived",
    "descendants",
    "alt_of",
    "form_of",
    "categories",
    "redirects",
}

TARGET_KEYS = {
    "word",
    "lang",
    "lang_code",
    "pos",
    "etymology_number",
    "etymology_text",
    "etymology_templates",
    "derived",
    "descendants",
    "alt_of",
    "form_of",
    "categories",
    "redirects",
    "literal_meaning",
    "wikidata",
}


def normalize_entry(entry: dict) -> dict:
    """
    Take a raw wiktextract entry dict and return a dict with only TARGET_KEYS
    plus node_id, with JSON fields serialized as strings.
    """
    out = {}

    # basic fields
    for key in TARGET_KEYS:
        value = entry.get(key)

        if key in JSON_FIELDS:
            # store as JSON string (or NULL if missing)
            out[key] = json.dumps(value) if value is not None else None
        else:
            out[key] = value

    out["node_id"] = make_node_id(entry)
    return out


def insert_entries_from_jsonl(db_path: str, jsonl_gz_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    insert_sql = """
    INSERT OR REPLACE INTO entries (
        node_id,
        word,
        lang,
        lang_code,
        pos,
        etymology_number,
        etymology_text,
        etymology_templates,
        derived,
        descendants,
        alt_of,
        form_of,
        categories,
        redirects,
        literal_meaning,
        wikidata
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    count = 0
    jsonl_path = Path(jsonl_gz_path)

    with gzip.open(jsonl_path, "rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                # skip malformed line
                continue

            # we only care about entries that have at least word + lang_code
            if "word" not in entry or "lang_code" not in entry:
                continue

            norm = normalize_entry(entry)

            params = (
                norm["node_id"],
                norm.get("word"),
                norm.get("lang"),
                norm.get("lang_code"),
                norm.get("pos"),
                norm.get("etymology_number"),
                norm.get("etymology_text"),
                norm.get("etymology_templates"),
                norm.get("derived"),
                norm.get("descendants"),
                norm.get("alt_of"),
                norm.get("form_of"),
                norm.get("categories"),
                norm.get("redirects"),
                norm.get("literal_meaning"),
                norm.get("wikidata"),
            )

            cur.execute(insert_sql, params)
            count += 1

            # occasional commit so we don't build up huge transaction
            if count % 5000 == 0:
                conn.commit()

    conn.commit()
    conn.close()
    print(f"Inserted {count} entries into {db_path}")

# ------------------------------------------------------------
# Edge helpers
# ------------------------------------------------------------

def pick_best(candidates, current_pos: str | None) -> str | None:
    """
    candidates: list of (node_id, pos, etymology_number)

    Strategy:
      1. If any match current_pos exactly, pick the first of those
         (preferring smallest etymology_number).
      2. Else, if any have etymology_number == 0 or None, pick the first of those.
      3. Else, pick the first candidate.

    Returns node_id or None.
    """
    if not candidates:
        return None

    # 1) POS match
    if current_pos:
        pos_matches = [row for row in candidates if (row[1] or "") == current_pos]
        if pos_matches:
            # sort by etymology_number: treat None as larger
            pos_matches.sort(key=lambda r: (r[2] is None, r[2]))
            return pos_matches[0][0]

    # 2) etymology_number == 0 or None
    ety0 = [r for r in candidates if (r[2] is None) or (r[2] == 0)]
    if ety0:
        return ety0[0][0]

    # 3) fallback: first
    return candidates[0][0]


def resolve_target_node(
    cur: sqlite3.Cursor,
    lang_code: str | None,
    word: str | None,
    current_pos: str | None,
) -> str | None:
    """
    Given a target lang_code + word, look up matching entries.node_id.
    Uses pick_best() to choose among multiple candidates.
    Returns node_id or None if unresolved.
    """
    if not lang_code or not word:
        return None

    rows = cur.execute(
        """
        SELECT node_id, pos, etymology_number
        FROM entries
        WHERE lang_code = ?
          AND word = ?
        """,
        (lang_code, word),
    ).fetchall()

    if not rows:
        return None

    return pick_best(rows, current_pos)


def iter_relation_items(entry: dict, field: str):
    """
    Generic iterator over relation lists like:
      - alt_of
      - form_of
      - redirects
      - derived
      - descendants

    Yields dicts with:
      - "word": str
      - "lang_code": str | None
    """
    items = entry.get(field) or []

    for item in items:
        if isinstance(item, dict):
            word = item.get("word")
            if not word:
                continue
            # Some use lang_code, some use lang, some nothing.
            lang_code = item.get("lang_code") or item.get("lang")
            yield {"word": word, "lang_code": lang_code}
        else:
            # bare string → just a word, no explicit language
            yield {"word": str(item), "lang_code": None}


# field_name, relation_type, use_pos_for_matching, fallback_to_entry_lang_if_missing
RELATION_SPECS = [
    ("alt_of",      "alt_of",     True,  True),
    ("form_of",     "form_of",    True,  True),
    ("redirects",   "redirect",   True,  True),
    ("derived",     "derived",    False, True),
    ("descendants", "descendant", False, False),
]


# ------------------------------------------------------------
# Main edge insertion pass
# ------------------------------------------------------------

def insert_edges_from_jsonl(db_path: str, jsonl_gz_path: str) -> None:
    """
    Second pass over the JSONL.gz file: create edges after all nodes exist.

    Creates edges for:
      - alt_of      (variant → canonical)
      - form_of     (inflected form → lemma)
      - redirects   (page → target)
      - derived     (this word → derived term)
      - descendants (this word → descendant term)

    etymology_templates are ignored in v1.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    insert_sql = """
    INSERT INTO edges (src_id, dst_id, relation_type)
    VALUES (?, ?, ?)
    """

    jsonl_path = Path(jsonl_gz_path)
    count_edges = 0
    count_entries = 0

    with gzip.open(jsonl_path, "rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            if "word" not in entry or "lang_code" not in entry:
                continue

            count_entries += 1
            src_id = make_node_id(entry)        # reuse your existing function
            current_pos = entry.get("pos")
            entry_lang = entry.get("lang_code")

            # handle all configured relation fields
            for field, relation_type, use_pos, fallback_entry_lang in RELATION_SPECS:
                for rel in iter_relation_items(entry, field):
                    target_word = rel["word"]
                    if not target_word:
                        continue

                    target_lang = rel["lang_code"]
                    if not target_lang and fallback_entry_lang:
                        target_lang = entry_lang

                    if not target_lang:
                        # can't resolve without language
                        continue

                    pos_for_match = current_pos if use_pos else None
                    dst_id = resolve_target_node(cur, target_lang, target_word, pos_for_match)
                    if not dst_id:
                        continue

                    cur.execute(insert_sql, (src_id, dst_id, relation_type))
                    count_edges += 1

            if count_entries % 2000 == 0:
                conn.commit()

    conn.commit()
    conn.close()
    print(f"Processed {count_entries} entries, inserted {count_edges} edges into {db_path}")




if __name__ == "__main__":
    init_db(DB_PATH)
    insert_entries_from_jsonl(DB_PATH, JSONL_GZ_PATH)
    insert_edges_from_jsonl(DB_PATH, JSONL_GZ_PATH)