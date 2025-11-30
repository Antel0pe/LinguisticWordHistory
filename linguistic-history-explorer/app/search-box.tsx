"use client";

import { useState } from "react";
import { getEtymologyChainForWord, type EntryRow } from "@/app/actions/etymology";

export function SearchBox() {
  const [word, setWord] = useState("");
  const [chain, setChain] = useState<EntryRow[]>([]);
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  async function handleSearch() {
    const trimmed = word.trim();
    if (!trimmed) return;

    setIsLoading(true);
    setError(null);
    setChain([]);
    setSelectedIndex(null);

    try {
      const result = await getEtymologyChainForWord(trimmed);
      setChain(result);
      if (result.length > 0) {
        // default-select the *searched* word (which will be last in display)
        setSelectedIndex(0);
      } else {
        setError("No etymology chain found for that word.");
      }
    } catch (e) {
      console.error(e);
      setError("Something went wrong searching the database.");
    } finally {
      setIsLoading(false);
    }
  }

  // We store chain as [newest, parent, parent, ...].
  // For display, reverse so it's [oldest, ..., newest].
  const displayChain = chain.slice().reverse();

  const selectedEntry =
    selectedIndex != null && selectedIndex >= 0 && selectedIndex < chain.length
      ? chain[selectedIndex]
      : null;

  return (
    <div style={{ maxWidth: 800 }}>
      {/* Search bar + button inline */}
      <div
        style={{
          display: "flex",
          gap: "0.5rem",
          marginBottom: "0.75rem",
          alignItems: "center",
        }}
      >
        <input
          value={word}
          onChange={(e) => setWord(e.target.value)}
          placeholder="Enter an English word, e.g. history"
          style={{
            flex: 1,
            padding: "0.4rem 0.6rem",
            fontSize: "0.95rem",
          }}
        />
        <button
          onClick={handleSearch}
          disabled={isLoading}
          style={{
            padding: "0.4rem 0.8rem",
            fontSize: "0.95rem",
            cursor: "pointer",
          }}
        >
          {isLoading ? "Searching..." : "Search"}
        </button>
      </div>

      {error && (
        <div style={{ color: "red", marginBottom: "0.5rem" }}>{error}</div>
      )}

      {displayChain.length > 0 && (
        <>
          {/* Chain: oldest -> ... -> newest (searched word last) */}
          <div
            style={{
              marginBottom: "0.75rem",
              fontFamily: "monospace",
              fontSize: "0.95rem",
            }}
          >
            {displayChain.map((entry, idx) => {
              const originalIndex = chain.length - 1 - idx; // map back to original array
              const isSelected = selectedIndex === originalIndex;

              return (
                <span key={entry.nodeId}>
                  {idx > 0 && " -> "}
                  <button
                    type="button"
                    onClick={() => setSelectedIndex(originalIndex)}
                    style={{
                      border: "none",
                      background: "none",
                      padding: 0,
                      cursor: "pointer",
                      textDecoration: "underline",
                      fontWeight: isSelected ? "bold" : "normal",
                      color: "#0645ad", // link-ish blue; optional
                    }}
                    title={entry.nodeId}
                  >
                    {entry.word}
                  </button>
                </span>
              );
            })}
          </div>

          {/* Scrollable JSON dump with black text */}
          <div
            style={{
              maxHeight: 300,
              overflow: "auto",
              border: "1px solid #ccc",
              borderRadius: 4,
              padding: "0.5rem",
              fontSize: "0.85rem",
              background: "#fafafa",
            }}
          >
            {selectedEntry ? (
              <pre style={{ margin: 0, color: "#000" }}>
                {JSON.stringify(selectedEntry, null, 2)}
              </pre>
            ) : (
              <span>Click a word in the chain to view the full EntryRow JSON.</span>
            )}
          </div>
        </>
      )}
    </div>
  );
}
