# polyscalp-live

Every 10 minutes GitHub Actions records the real order book (top 5 bids/asks) of every open
Polymarket city-temperature market, plus the latest airport METAR reports.
No wallet, no keys, no trading — public market data only.

Data: `data/YYYY-MM-DD/books-HHMM.jsonl.gz` and `metar-HHMM.jsonl.gz` (gzip JSON lines).
