-- Schema for the PostgreSQL baseline (experiment E-3). Applied once by the image's
-- /docker-entrypoint-initdb.d on a fresh volume; cmd/pgload truncates and reloads the tables.
--
-- Two variants of the same corpus live side by side in one database, so they share one server,
-- one buffer pool and one configuration and differ only in how the value is stored:
--
--   logs       PRE-STRUCTURED. latency_ms is extracted at insert by a generated column and has
--              a B-tree. This is what a DBA would actually deploy for a field known up front,
--              and it needs exactly the up-front knowledge logd's template needs.
--   logs_raw   UNSTRUCTURED. Raw lines only; the value is extracted at query time. This is
--              PostgreSQL's equivalent of a scan, and the fallback for a question nobody
--              anticipated (the same role as logd's forced scan).
--
-- The extraction expression below must stay byte-identical to the one the shim uses for
-- logs_raw (internal/pgdata); a test pins that. It uses the same regular expression as the
-- corpus manifest's ground truth (loggen's tookRe), and like it takes the FIRST occurrence.

-- One row per distinct label set. Storing the labels inline as jsonb would cost 58 bytes per
-- row for {app, env, region} -- about a third of the table for this corpus -- and repeat the
-- same three strings 500,000 times. Every log store in this comparison keeps a stream
-- dictionary rather than per-line labels, and a DBA would normalise them the same way.
CREATE TABLE streams (
    id     integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    labels jsonb   NOT NULL UNIQUE
);

-- Variant 1: pre-structured.
--
-- ts_ns is bigint nanoseconds rather than timestamptz on purpose: timestamptz has microsecond
-- resolution, and the manifest's primary key is ts_ns, so a row could no longer be diffed
-- against ground truth exactly. Range partitioning works on bigint the same way.
--
-- The generated column makes PostgreSQL pay for its own extraction at insert, which is the
-- fair counterpart of logd extracting at ingest; extracting in the loader would move that cost
-- out of the database and out of the measurement. integer fits every latency the corpus can
-- produce. Note the failure mode: a value that does not fit makes the INSERT fail and rejects
-- the whole COPY batch, where logd records a template failure and stores the line unindexed.
--
-- No foreign key from stream_id to streams: it would add an index probe to every inserted row
-- on the hot path, and the loader is the only writer. Column order puts the fixed-width
-- columns first so the row needs no alignment padding before the text.
CREATE TABLE logs (
    ts_ns      bigint  NOT NULL,
    stream_id  integer NOT NULL,
    latency_ms integer GENERATED ALWAYS AS ((substring(line FROM 'took=([0-9]+)ms'))::integer) STORED,
    line       text    NOT NULL
);

-- The value index. The measured query is
--     WHERE latency_ms > $1 AND ts_ns >= $2 AND ts_ns < $3 ORDER BY ts_ns DESC LIMIT $4
--   * latency_ms leads because the value predicate is the selective one.
--   * ts_ns second lets the time window be checked inside the index, before any heap visit.
--     That changes nothing when the window covers the whole table (as in the testbed), and is
--     what keeps "slow requests in the last hour" cheap when the table holds weeks. It cannot
--     supply the ORDER BY (the leading column is a range), so the sort stays either way.
--   * Partial: 19% of the corpus lines carry no took= at all. Every comparison on latency_ms
--     implies latency_ms IS NOT NULL, so the planner can use this index for any of them.
-- Considered and rejected: (latency_ms) alone, smaller but must visit the heap to apply the
-- window; (ts_ns, latency_ms), which scans the whole window for a selective value; an
-- expression index on logs_raw instead of a stored column, which is the right tool for adding
-- a field to an EXISTING table (CREATE INDEX CONCURRENTLY, no rewrite -- ALTER TABLE ... ADD
-- COLUMN ... GENERATED ... STORED rewrites the whole table under an exclusive lock), but makes
-- every query repeat the expression verbatim.
CREATE INDEX logs_latency ON logs (latency_ms, ts_ns) WHERE latency_ms IS NOT NULL;

-- Time order: the "last N lines" every log UI opens with, and the access path for retention
-- deletes. A BRIN would be far smaller (ts_ns is perfectly correlated with insert order) but
-- cannot serve ORDER BY ts_ns DESC LIMIT n. A workload that filters by stream would make this
-- (stream_id, ts_ns) instead; the measured queries do not filter by label.
CREATE INDEX logs_ts ON logs (ts_ns);

-- Variant 2: unstructured. Same columns minus the extracted value, same time index -- a DBA
-- who stores raw lines still indexes time.
CREATE TABLE logs_raw (
    ts_ns     bigint  NOT NULL,
    stream_id integer NOT NULL,
    line      text    NOT NULL
);
CREATE INDEX logs_raw_ts ON logs_raw (ts_ns);

-- Retention, not implemented here. A production deployment would declare these tables
--     PARTITION BY RANGE (ts_ns)
-- with one partition per day, and expire data with DETACH/DROP PARTITION -- instant, and no
-- VACUUM debt -- instead of DELETE. That is the counterpart of logd deleting whole segments.
-- It is left out because the testbed corpora span minutes (500k lines at 1 ms spacing), so
-- every query would hit exactly one partition and partitioning would change nothing measured
-- except planning overhead.
