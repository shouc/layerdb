# SPFresh System Architecture on LayerDB

Date: 2026-02-17

## Goal
Build a full SPFresh system architecture that uses existing LayerDB for durability while keeping a fast in-memory ANN serving path.

## Components

### 1) Foreground Updater (`SpFreshLayerDbIndex::upsert/delete`)
Responsibilities:
- validate input dimensionality
- persist latest vector state in LayerDB
- apply immediate in-memory SPFresh update for low-latency availability
- track pending maintenance work and notify background rebuilder

Implementation details:
- persisted vector key format: `spfresh/v/{id}`
- payload: serialized `VectorRecord`
- updates and deletes are gated by a shared mutex so persistence and in-memory mutation stay ordered

### 2) Background Rebuilder (`spawn_rebuilder` + `rebuild_once`)
Responsibilities:
- run asynchronously from foreground updates
- rebuild postings from durable LayerDB state when triggered
- atomically swap rebuilt index into serving path

Trigger modes:
- signal-based (`pending_ops` reaches threshold)
- optional interval checks
- explicit synchronous trigger (`force_rebuild`) for benchmark checkpoints

### 3) LayerDB Block/Storage Controller
LayerDB is used as the persistent block controller equivalent:
- WAL-backed durable writes
- ordered keyspace scan for recovery/rebuild
- compacted LSM storage under sustained updates

SPFresh logic remains in-memory for query latency; LayerDB is the source of truth for reconstruction.

### 4) Recovery Path (`SpFreshLayerDbIndex::open`)
Responsibilities:
- open LayerDB
- refresh snapshot visibility
- scan `spfresh/v/*` rows
- rebuild `SpFreshIndex` from recovered vectors

This provides crash recovery without replaying external logs in `vectordb`.

## Data Flow
1. Client update arrives.
2. Updater persists vector state in LayerDB.
3. Updater applies in-memory SPFresh mutation.
4. Updater increments pending counter.
5. Rebuilder receives signal and rebuilds index from LayerDB snapshot.
6. Search serves from latest in-memory index.

## Relationship to SPFresh Paper Architecture (arXiv:2410.14452v1)
This implementation follows the same system split:
- updater: low-latency foreground ingestion/search continuity
- rebuilder: asynchronous heavy maintenance work
- block controller: durable storage abstraction (mapped to LayerDB)
- recovery: restore serving state from durable storage

Code reference:
- `vectordb/src/index/spfresh_layerdb.rs`
- `vectordb/src/index/spfresh.rs`
