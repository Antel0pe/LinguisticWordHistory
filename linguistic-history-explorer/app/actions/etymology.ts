// app/actions/etymology.ts
"use server";

import path from "path";
import Database from "better-sqlite3";

/**
 * Compute DB path once. You can also make this an env var:
 *   process.env.ETYMOLOGY_DB_PATH
 */
const DB_PATH =
  process.env.ETYMOLOGY_DB_PATH ??
  // 2. Fallback: one level up from the Next.js app dir
  path.resolve(process.cwd(), "..", "etymology.db");


// Reuse a single connection in dev to avoid too many open files on HMR.
let _db: Database.Database | null = null;

function getDb(): Database.Database {
  if (_db) return _db;

  const db = new Database(DB_PATH, {
    readonly: true,
    fileMustExist: true,
  });

  _db = db;
  return db;
}

/**
 * Types that roughly mirror your schema (entries + edges)
 */
export type EntryRow = {
  nodeId: string;
  word: string;
  lang: string | null;
  langCode: string;
  pos: string | null;
  etymologyNumber: number | null;
  etymologyText: string | null;

  // JSON fields, parsed for convenience
  etymologyTemplates: unknown | null;
  derived: unknown | null;
  descendants: unknown | null;
  altOf: unknown | null;
  formOf: unknown | null;
  categories: unknown | null;
  redirects: unknown | null;

  literalMeaning: string | null;
  wikidata: string | null;
};

export type EdgeRow = {
  id: number;
  srcId: string;
  dstId: string;
  relationType: string;
};

export type Neighbor = {
  nodeId: string;
  word: string;
  langCode: string;
  pos: string | null;
  relationType: string;
};

/**
 * Internal helper: parse JSON columns that are stored as TEXT.
 */
function parseJsonField(value: unknown): unknown | null {
  if (value == null) return null;
  if (typeof value !== "string") return null;
  if (value === "") return null;

  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function mapEntryRow(raw: any): EntryRow {
  return {
    nodeId: raw.node_id,
    word: raw.word,
    lang: raw.lang,
    langCode: raw.lang_code,
    pos: raw.pos,
    etymologyNumber: raw.etymology_number,
    etymologyText: raw.etymology_text,

    etymologyTemplates: parseJsonField(raw.etymology_templates),
    derived: parseJsonField(raw.derived),
    descendants: parseJsonField(raw.descendants),
    altOf: parseJsonField(raw.alt_of),
    formOf: parseJsonField(raw.form_of),
    categories: parseJsonField(raw.categories),
    redirects: parseJsonField(raw.redirects),

    literalMeaning: raw.literal_meaning,
    wikidata: raw.wikidata,
  };
}

/**
 * Basic search: all entries for a given word.
 * Uses idx_entries_word or idx_entries_word_lang.
 */
export async function findEntriesByWord(
  word: string,
  opts?: { langCode?: string; limit?: number }
): Promise<EntryRow[]> {
  const db = getDb();
  const langCode = opts?.langCode ?? null;
  const limit = opts?.limit ?? 50;

  if (!word.trim()) return [];

  let rows: any[];

  if (langCode) {
    // Uses composite index: idx_entries_word_lang (word, lang_code)
    const stmt = db.prepare(
      `
      SELECT *
      FROM entries
      WHERE word = ?
        AND lang_code = ?
      ORDER BY etymology_number ASC, pos ASC
      LIMIT ?
      `
    );
    rows = stmt.all(word, langCode, limit);
  } else {
    // Uses idx_entries_word
    const stmt = db.prepare(
      `
      SELECT *
      FROM entries
      WHERE word = ?
      ORDER BY lang_code ASC, etymology_number ASC, pos ASC
      LIMIT ?
      `
    );
    rows = stmt.all(word, limit);
  }

  return rows.map(mapEntryRow);
}

/**
 * Lookup a single entry by node_id (your canonical key).
 */
export async function getEntryByNodeId(
  nodeId: string
): Promise<EntryRow | null> {
  const db = getDb();
  const stmt = db.prepare(
    `
    SELECT *
    FROM entries
    WHERE node_id = ?
    `
  );
  const row = stmt.get(nodeId);
  if (!row) return null;
  return mapEntryRow(row);
}

/**
 * Get all outgoing edges from a node.
 * Uses idx_edges_src.
 */
export async function getOutgoingEdges(
  nodeId: string
): Promise<EdgeRow[]> {
  const db = getDb();
  const stmt = db.prepare(
    `
    SELECT id, src_id, dst_id, relation_type
    FROM edges
    WHERE src_id = ?
    ORDER BY relation_type ASC, id ASC
    `
  );
  const rows = stmt.all(nodeId) as any[];

  return rows.map((r) => ({
    id: r.id,
    srcId: r.src_id,
    dstId: r.dst_id,
    relationType: r.relation_type,
  }));
}

/**
 * Get all incoming edges to a node.
 * Uses idx_edges_dst.
 */
export async function getIncomingEdges(
  nodeId: string
): Promise<EdgeRow[]> {
  const db = getDb();
  const stmt = db.prepare(
    `
    SELECT id, src_id, dst_id, relation_type
    FROM edges
    WHERE dst_id = ?
    ORDER BY relation_type ASC, id ASC
    `
  );
  const rows = stmt.all(nodeId) as any[];

  return rows.map((r) => ({
    id: r.id,
    srcId: r.src_id,
    dstId: r.dst_id,
    relationType: r.relation_type,
  }));
}

/**
 * Convenience: neighbors from a node, joined with entries on the other side.
 * Direction: src → dst (derived, descendant, alt_of, etc.).
 */
export async function getOutgoingNeighbors(
  nodeId: string
): Promise<Neighbor[]> {
  const db = getDb();
  const stmt = db.prepare(
    `
    SELECT
      e2.node_id   AS node_id,
      e2.word      AS word,
      e2.lang_code AS lang_code,
      e2.pos       AS pos,
      ed.relation_type AS relation_type
    FROM edges ed
    JOIN entries e2
      ON ed.dst_id = e2.node_id
    WHERE ed.src_id = ?
    ORDER BY ed.relation_type ASC, e2.lang_code ASC, e2.word ASC
    `
  );

  const rows = stmt.all(nodeId) as any[];

  return rows.map((r) => ({
    nodeId: r.node_id,
    word: r.word,
    langCode: r.lang_code,
    pos: r.pos,
    relationType: r.relation_type,
  }));
}

/**
 * Convenience: neighbors pointing into this node (dst ← src).
 */
export async function getIncomingNeighbors(
  nodeId: string
): Promise<Neighbor[]> {
  const db = getDb();
  const stmt = db.prepare(
    `
    SELECT
      e1.node_id   AS node_id,
      e1.word      AS word,
      e1.lang_code AS lang_code,
      e1.pos       AS pos,
      ed.relation_type AS relation_type
    FROM edges ed
    JOIN entries e1
      ON ed.src_id = e1.node_id
    WHERE ed.dst_id = ?
    ORDER BY ed.relation_type ASC, e1.lang_code ASC, e1.word ASC
    `
  );

  const rows = stmt.all(nodeId) as any[];

  return rows.map((r) => ({
    nodeId: r.node_id,
    word: r.word,
    langCode: r.lang_code,
    pos: r.pos,
    relationType: r.relation_type,
  }));
}

// app/actions/etymology.ts

/**
 * Find the "canonical" English entry for a word:
 * prefer (lang_code='en', pos='noun', etymology_number=0),
 * fall back to the first English entry if not found.
 */
async function findCanonicalEnglishEntry(
  word: string
): Promise<EntryRow | null> {
  const rows = await findEntriesByWord(word, { langCode: "en" });
  if (!rows.length) return null;

  const preferred = rows.find(
    (r) => r.pos === "noun" && (r.etymologyNumber ?? 0) === 0
  );
  return preferred ?? rows[0];
}

/**
 * Build a descendant chain starting from an English entry of `word`.
 * Follows edges with relation_type = 'descendant', depth-first,
 * avoiding cycles.
 */
export async function getDescendantChainForWord(
  word: string,
  opts?: { maxDepth?: number }
): Promise<EntryRow[]> {
  const start = await findCanonicalEnglishEntry(word);
  if (!start) return [];

  const db = getDb();

  // Only descendants from this node
  const neighborStmt = db.prepare(
    `
    SELECT e2.*
    FROM edges ed
    JOIN entries e2
      ON ed.dst_id = e2.node_id
    WHERE ed.src_id = ?
      AND ed.relation_type = 'descendant'
    ORDER BY e2.lang_code ASC, e2.word ASC, e2.pos ASC, e2.etymology_number ASC
    `
  );

  const visited = new Set<string>();
  const chain: EntryRow[] = [];
  const maxDepth = opts?.maxDepth ?? 64;

  function dfs(node: EntryRow, depth: number) {
    if (visited.has(node.nodeId)) return;
    visited.add(node.nodeId);
    chain.push(node);

    if (depth >= maxDepth) return;

    const rawNeighbors = neighborStmt.all(node.nodeId) as any[];
    for (const raw of rawNeighbors) {
      const next = mapEntryRow(raw);
      dfs(next, depth + 1);
    }
  }

  dfs(start, 0);
  return chain;
}

export async function getEtymologyChainForWord(
  word: string,
  opts?: { maxDepth?: number }
): Promise<EntryRow[]> {
  const trimmed = word.trim();
  if (!trimmed) return [];

  const db = getDb();

  // Step 1: find the English starting entry.
  // You can tweak this to be stricter/looser if you want.
  const candidates = await findEntriesByWord(trimmed, { langCode: "en", limit: 20 });
  if (!candidates.length) return [];

  // Prefer noun, etymology_number = 0 (or null), otherwise fallback to first.
  const start =
    candidates.find(
      (r) =>
        r.pos === "noun" &&
        (r.etymologyNumber == null || r.etymologyNumber === 0)
    ) ?? candidates[0];

  // Step 2: prepare a statement to look for "parent" entries:
  // parent --[descendant]--> current
  const parentStmt = db.prepare(
    `
    SELECT e1.*
    FROM edges ed
    JOIN entries e1
      ON ed.src_id = e1.node_id
    WHERE ed.dst_id = ?
      AND ed.relation_type = 'descendant'
    ORDER BY e1.lang_code ASC, e1.word ASC, e1.pos ASC, e1.etymology_number ASC
    `
  );

  const chain: EntryRow[] = [];
  const visited = new Set<string>();
  const maxDepth = opts?.maxDepth ?? 64;

  let current: EntryRow | null = start;
  let depth = 0;

  while (current && !visited.has(current.nodeId) && depth <= maxDepth) {
    chain.push(current);
    visited.add(current.nodeId);

    const rawParents = parentStmt.all(current.nodeId) as any[];
    if (!rawParents.length) {
      break; // no more ancestors
    }

    // For now, just pick the "first" parent. If you want branching later, we can change this.
    const parent = mapEntryRow(rawParents[0]);
    current = parent;
    depth += 1;
  }

  return chain;
}