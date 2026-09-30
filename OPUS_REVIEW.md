# PerunCS Cluster — Review and Implementation Spec (Opus), revision 110

**Baseline:** commit `9975d95`. Revision 6 was the original static review; later revisions record
implementation follow-up and remaining acceptance gates.

## 0. Scope, conventions, revision history

### What PerunCS Cluster is (governs every decision below)

PerunCS Cluster is an **embedded, Eclipse Store-based clustering library**: one writer node and N
reader nodes, replicated over Aeron.

- **Its API is Eclipse Store's API.** An Eclipse Store application switches to a cluster by
  changing how it obtains its `StorageManager`, not how it uses it.
- **It is not a framework and not a feature port.** Eclipse Data Grid was supplied as a
  *reference only*: no Data Grid feature, name or SPI is adopted unless it independently serves
  the embedded library.
- **Deployments are examples, not requirements.** Helidon behind Caddy / a Cloudflare tunnel is
  one example deployment. No decision may depend on a web framework, proxy or configuration
  system, and configuration must be pluggable from any source.

### Conventions

- **Paths.** `M/` means `src/main/java/peruncs/cluster/`, `T/` means `src/test/java/peruncs/cluster/`,
  and `$GITHUB_ROOT/…` are upstream checkouts. A path starting with `src/` is repo-relative.
- **Severity.** P0 = correctness/data loss/availability; P1 = major performance, robustness or API
  contract; P2 = simplification or rule violation; P3 = cosmetic.
- **Compatibility.** `AGENTS.md` requires no backward compatibility: format changes mean "reseed
  on upgrade" and no migration code.
- **Decision provenance.** "Default decision" = chosen by this spec because nothing settled it;
  applied unless the owner objects.

### Revision history

- **rev 1:** the review.
- **rev 2:** owner decisions (no failover, drop-in API, mark in the Store).
- **rev 3:** the spec.
- **rev 4:** deployment neutrality, PR #832.
- **rev 6 (this revision):**
  - The old AGENTS.md rule 31 ("prefer Agrona/Serializer memory utilities") is deleted by the
    owner. Old rules 32–34 are now 31–33.
  - Added the limited FFM adoption policy (D-29) with two spec blocks: **F1**, bounds-checked
    parsing of replicated bytes, and **F2**, an arena-backed buffer pool spike folded into N1.
  - Added N3 (retention never triggered), found while writing the rev-5 documentation.
- **rev 7 (implementation sweep):**
  - Implemented C9 backup outcome reporting, including separate post-publication maintenance
    failures; implemented writer-driven N3 retention; removed the D-17 pause/resume control API.
  - Kept S1's same-name archive checks after tracing the crash-retry and conflicting-content
    cases: removing them can overwrite a durable backup. Digest inspection stays outside the
    publication lock and rechecks the destination stamp before publishing.
  - Confirmed the protocol has no node authentication or transport encryption and needs neither.
    P1-1/F1 code and parser checks are present; their remaining performance benchmark gate is
    not claimed complete.
- **rev 8 (performance sweep, 2026-09-28):**
  - Replaced FFM header reads with bounds-checked native-order `ByteBuffer.getLong` after a fair
    JMH comparison showed higher throughput at 64 KiB and no meaningful difference at 1 MiB.
    Kept the 5% raw-iterator gate open; the measured gap remains above its threshold.
  - The scanner now requires normalized native-order direct buffers. `Binary` views set native
    order on a duplicate, leaving caller buffer state untouched. The benchmark calls the
    production scanner directly.
  - Switched `Binary` scanning to Serializer's `iterateEntityData` callback instead of visiting
    channels and copying each `buffers()` array. The GC-profiled production scan had no collections
    and allocation at or near zero within profiler error.
  - The writer pre-filter now visits only type ids. The same-work benchmark compares that path to
    a raw type-id walk; it still misses the 5% gate. A regression test checks the type-only visitor
    and malformed framing.
- **rev 9 (2026-09-28):**
  - Added the S-2 Store atomicity crash matrix to the crashmatrix profile: 200 forked commits,
    each carrying a 4 MiB payload and named mark in one `Storer.commit()`, killed after a uniform
    0–50 ms delay inside the Store target. Source is uncompiled and unexecuted; repeat with the
    production `ReplicationMark` after A1 lands.
- **rev 10 (2026-09-28):**
  - Added the production `ReplicationMark` identity type and registered its reserved named root
    from the transport before replicated Store startup. A focused root-registration test is added
    but remains uncompiled and unrun. The guarded writer facade now includes and updates the mark
    on `store`, `storeAll`, `storeRoot`, persistence-manager writes, and explicit storer commits;
    it rejects raw-target and metadata writes. A persistence/reopen test is added but remains
    uncompiled and unrun. Target object-id verification, sequence reservation, bootstrap commit,
    recovery, reader resume, and legacy checkpoint/cursor removal remain open.
- **rev 11 (2026-09-28):**
  - The writer now reserves the mark's exact publisher sequence under coordinator admission and
    samples its recording id and start position while Archive maintenance is excluded. The target
    consumes that same sequence; Serializer failures cancel an unconsumed reservation. Production
    target writes now check for the reserved mark object id in the same header scan that filters
    index-relevant commits. The guarded writer, reader, and backup-reader facades hide the reserved
    root; standalone Aeron nodes do not register one. Focused scan and facade tests are added but
    remain uncompiled and unrun. Bootstrap, writer recovery, reader resume, and checkpoint/cursor
    deletion remain open.
- **rev 12 (2026-09-28):**
  - Writer startup now persists a missing mark only when the Store has no committed writer boundary
    and the Archive recording is empty. Existing marks are checked against Store object identity,
    cluster/store/epoch identity, recording, fencing token, sequence, and position. This bootstrap
    path and its failure cases still need integration tests and execution; replay-based recovery and
    reader resume remain open.
- **rev 13 (2026-09-28):**
  - Production readers now take their replay sequence, prepare position, and fencing floor from the
    registered Store mark. They skip replayed frames through that sequence, and reader startup no
    longer treats the `offset` file as its resume authority. Startup rejects a reader Store without
    a committed mark. The production reader no longer writes the `.reader-inflight` uncertainty
    record. Source and focused test edits are uncompiled and unrun. The legacy cursor file/listener,
    backup manifest cursor, writer checkpoints, writer tail replay, recovery matrix, and complete
    mark-identity checks remain open A1 work.
- **rev 14 (2026-09-28):**
  - Removed the lifecycle-owned `offset` file and applied-listener wiring. Reader Store validation
    now happens after opening the Store and requires its committed mark. Backup restore no longer
    compares a local cursor or deletes/replaces an existing reader Store; it installs a compatible
    seed only when the Store is absent. The old cursor/manifest formats remain in the backup
    backend and transitional transport API, and the code and affected tests are not yet compiled.
- **rev 15 (2026-09-28):**
  - Re-audited the rev-14 reader and lifecycle edits. Operator-facing seed/restart messages now say
    “Store mark” where they mean the persisted replication boundary. A1 remains partial: the writer
    still reconciles `AeronReplicationCheckpoint` instead of replaying the Archive tail; the legacy
    delivery listener, recorded-position durability gate, cursor file classes, and backup ZIP
    manifest still exist. No A1 deletion or recovery behavior is claimed complete by this wording
    cleanup. This source state is not compiled or tested.
- **rev 16 (2026-09-28):**
  - Removed the A1.6 live recorded-position gate and its background refresher, the reader delivery
    listener/uncertainty sidecar, and the obsolete live-gate configuration setting. Store imports
    already commit their replication mark atomically, so the extra Archive query and one reader
    thread are redundant. Deleted the crash tests whose only oracle was the removed sidecar; the
    production-mark crash matrix and A1.5 writer-recovery matrix are still required. A1 remains
    partial and this source state is uncompiled and untested.
- **rev 17 (2026-09-28):**
  - Replay now reads only the sequence field before discarding frames already covered by the Store
    mark. This avoids checksum and payload work for duplicate frames while preserving full
    validation for new sequences. A focused test corrupts duplicate header checksums to prove the
    skip happens before decoding; compile and execution remain pending. A1.5 recovery, checkpoint
    removal, and backup-manifest cleanup remain open.
- **rev 18 (2026-09-28):**
  - Writer startup now selects the recording from the Store mark (or the newest stream recording
    before the first mark), scans only the A1.5 bounded tail, validates envelope identity/chunks/CRC,
    and extends the recording after appending any required COMMIT/ABORT markers under the held writer
    lease. Writer coordination is deferred until a Store operation so it sees the hydrated mark;
    backup identity lookup no longer starts an uninitialized writer. This path is not compiled or
    tested yet. Checkpoint journal writes/classes and their crash tests remain and still need removal.
- **rev 19 (2026-09-28, dead-code sweep):**
  - Deleted the legacy cursor file/store/listener, checkpoint journal and write phase, backup ZIP
    manifest, and unused reader crash-milestone scaffolding. The live Aeron position and watermark
    codec moved from the obsolete `checkpoint` package to `storage.aeron.position`; generic cursor
    APIs remain scheduled for A3. The 200-trial Store-mark crash matrix now uses production
    `ReplicationMark`; execution and the A1.5 recovery matrix remain open.
- **rev 20 (2026-09-28, cleanup):**
  - Removed the redundant test-only `WriterCrashHooks` forwarding class; callers use the shared
    `CrashHook` directly. Removed an empty crash-matrix Maven properties block and stale checkpoint
    wording. The production-mark crash-matrix imports were corrected; source remains uncompiled.
- **rev 21 (2026-09-28, dead-code sweep):**
  - Removed the unused replication stream-name setting, transport parameters, and one-stream claim.
    Replaced a private single-use write callback with `Supplier`. Test compilation, 25 focused
    transport/bootstrap tests, and 28 coordinator tests pass. The typed A3 position replacement
    remains open; the standalone `none` transport keeps its null-free implementation.
- **rev 22 (2026-09-28, typed-position and dead-code sweep):**
  - Deleted `ReplicationCursor` and `AeronReplicationCursor`; all position providers now return a
    direct `ReplicationPosition`, avoiding the per-read byte-array/hex wrapper. The record includes
    `nodeId`, required to identify remote reader watermarks and backup publishers. The shared codec
    remains only for the Aeron watermark wire format.
  - Removed the unused transport `id()` API and its downstream string checks. Runtime replication
    presence now follows the typed Store mark; string parsing remains only at the external settings
    boundary until configuration is typed. `ReplicationPosition.NONE` maps to unknown backup fields.
    Test compilation and 113 focused tests pass; the full test gates remain open.
- **rev 23 (2026-09-28, API dead-code sweep):**
  - Added the `ClusterStorage`/immutable `ClusterStorageFoundation` entry point and removed
    `NodeOptions` plus `ClusterNode.open`. Removed the unneeded `Foundation(rootSupplier)` overload
    and inlined its one-use assembly-start helper; callers use the specified `Foundation()` fluent
    API. `mvn -DskipTests test-compile` and `NodeAssemblyLifecycleTest` (17 tests) pass; D1 still needs to replace `NodeSettingsSource` with typed
    `NodeConfig` and `PERUNCS_` settings.
  - Rechecked disconnected production classes against the source. The graph's only zero-degree
    production candidates are required nested Store adapters; no further production types were
    deleted based on the graph alone. Historical names in this file remain as audit evidence.
- **rev 24 (2026-09-28, legacy-setting cleanup):**
  - Deleted unprefixed/`MSCNL_*` environment fallbacks and the obsolete durability-mode alias.
    Tests now assert that the old aliases are ignored. `PERUNCS_` remains temporarily;
    D1 still needs typed `NodeConfig`, centralized settings, and the `PERUNCS_` names.
- **rev 25 (2026-09-29, role cleanup):**
  - Removed the duplicate `IS_BACKUP_NODE` boolean, its strict boolean parser, conflict-resolution
    branch, and public role-name constants on `NodeSettingsSource`. Blank role now means writer;
    `backup-reader` is the only backup-role setting. Focused configuration, role, seed, Aeron, and
    lifecycle suites pass (56 tests).
- **rev 26 (2026-09-29, dead-setting sweep):**
  - Removed the unused `NodeSettingsSource.EnvKeys.NODE_ID`; the transport has always read its
    distinct `PERUNCS_AERON_NODE_ID` key. No runtime or test caller referenced the dead
    constant.
- **rev 27 (2026-09-29, F1 benchmark and hot path):**
  - Restored a runnable F1 benchmark gate: `-Pbench` compiles `src/bench/java`, runs JMH's
    annotation processor, and `bench/run-f1.sh` plus `bench/compare-f1.py` exercise the actual
    production `Binary` overload. The prior comparator measured only the direct `ByteBuffer` helper.
  - Split the writer type-only walk from full entity visitation to remove its per-entity mode
    branch. Native-order `ChunksWrapper` input now uses absolute reads directly, without allocating
    a normalized duplicate view.
  - The two-fork Java 27 diagnostic run measured checked/raw throughput at 7.91/8.09 M ops/s for
    64 KiB and 405/456 k ops/s for 1 MiB. The short run passes 64 KiB (2.2% slower) but fails the
    1 MiB gate (11.2% slower) and has wide confidence intervals. F1 remains partial; the longer
    acceptance run and final parser tests remain required.
  - Source inspection confirms retention caps deletion at the durable quorum watermark and returns
    `NOTHING_TO_DELETE` for incomplete or behind quorums. Unit tests cover both controller cases;
    the writer-plus-two-readers scheduled-maintenance integration test remains open.
- **rev 28 (2026-09-29, D1 environment prefix):**
  - Renamed active configuration keys from `ECLIPSE_DATAGRID_` to `PERUNCS_` across source, tests,
    README, and current spec text. The old prefix is no longer read; typed `NodeConfig`, a single
    parser/default `Setting` enum, and migration of temporary setting names remain open.
- **rev 29 (2026-09-29, storage-size refresh):**
  - Removed virtual-thread creation from `StorageUsageGauge` reads. The node's existing maintenance
    worker refreshes the cached size every five seconds; health and status reads only return that
    snapshot. The writer's limit check now consumes the refreshed value instead of walking the Store
    directory again. Focused and full-suite verification remain pending.
- **rev 30 (2026-09-29, graph-section consolidation):**
  - Routed graph reads and writes through one lock/admission/validity helper. Only replication write
    sections invalidate the graph when their callback fails; application-exclusive sections retain
    explicit invalidation ownership. Focused and full-suite verification remain pending.
- **rev 31 (2026-09-29, position API naming):**
  - Renamed the replication-applier API from `cursor()` to `position()` and aligned backup position
    suppliers and test helpers with `ReplicationPosition`. The `CursorSnapshot` type remains a
    private reader-delivery snapshot, not a replication-position API. Verification remains pending.
- **rev 32 (2026-09-29, backup staging copy):**
  - Uploaded archive staging uses `FileChannel.transferTo` with a 1 MiB direct-buffer fallback if
    the channel reports no progress, and rejects size changes during the copy. Existing upload
    bounds, validation, and digest checks remain. Backup digest generation is still a separate
    directory pass; verification remains pending.
- **rev 33 (2026-09-29, role parsing):**
  - Lifecycle startup and recovery now use the role parsed once by `NodeAssembly`; production Aeron
    settings receive that same role instead of reparsing it. Role-specific assembly and close-order
    cleanup remain open. Verification remains pending.
- **rev 34 (2026-09-29, stop-result sentinels):**
  - `ReplicationApplier.StopResult` validates its `-1` unknown-value sentinels and exposes
    `hasSequence()` / `hasPosition()`, completing D-16 for that public record. Verification remains
    pending.
- **rev 35 (2026-09-29, backup digest pass):**
  - Backup compression calculates CRC32C over sorted Store paths and bytes while streaming each
    file into the ZIP, then writes the identity entry with that digest. This removes the separate
    pre-compression directory read. The backup identity version advances to distinguish the new
    CRC32C digest. Focused and full verification remain pending.
- **rev 36 (2026-09-29, scanner visibility):**
  - `EntityHeaders` and its framing method are now package-private. The materializer reaches the
    check through the existing internal `ClusterStoreIndexes` facade, whose narrow bridge exposes
    only framing validation across packages. Verification remains pending.
- **rev 37 (2026-09-29, upstream and version-pin recheck):**
  - Rechecked eclipse-store PR #832: it remains open. The interim reflection boundary and the
    upstream adoption gate therefore remain in place. Aeron mark-file message matching now names
    the pinned dependency version, 1.53.1; its exact-message tests already exist.
- **rev 38 (2026-09-29, typed public role and scanner recheck):**
  - Moved `NodeRole` into the exported `api` package and changed `NodeStatus` from a writer boolean
    to the typed role required by D-19. Role parsing now accepts the setting value directly rather
    than depending on the settings-source interface. Transport `none` now resolves to the distinct
    `STANDALONE` role, which writes its local Store and reports no replication. This removes the
    internal role type from the exported status signature; A7's remaining implementation dependency
    cleanup is still open. Added and passed the focused `STANDALONE` + `NOT_CONFIGURED`
    public-contract test.
  - Fixed two cross-package visibility errors found by compiling the benchmark profile. The role
    accessor and transport constructor are public only inside non-exported implementation packages.
  - Rechecked the current F1 parser against the Serializer raw walk on Java 27. In the latest
    two-fork diagnostic, bounds-checked NIO measured 7.997 M vs 9.689 M ops/s at 64 KiB (17.5%
    slower) and 415.2 k vs 447.7 k at 1 MiB (7.3% slower); both gates miss, and the 1 MiB intervals
    overlap. Relative reads and the bounded FFM candidate were slower. The reusable callback adapter
    did not reduce measured allocation and regressed the 1 MiB result. Those extra variants were
    removed; the longer acceptance run remains open.
  - `mvn -q -DskipTests test-compile` passes after the role move. JUnit, integration, soak,
    crashmatrix, and final benchmark acceptance remain pending.
- **rev 39 (2026-09-29, F1 scanner measurement):**
  - The type-id hot path now reads through a native-order `ByteBuffer` VarHandle; the full header
    walk retains absolute `ByteBuffer` reads. This keeps framing checks and avoids raw addresses.
    The benchmark runner no longer carries the slower experimental FFM scanners.
  - `EntityHeadersTest` passes against the current scanner, including seeded corrupt-length fuzz
    and comparison with the upstream iterator on 10,000 real Serializer entities.
  - A three-fork Java 27 diagnostic measured the production Binary scan at 8.482 M vs 9.115 M
    raw ops/s for 64 KiB (6.9% slower), and 398.6 k vs 451.3 k for 1 MiB (11.7% slower). Both
    5% gates remain open; this Mac run does not satisfy D-25's Linux/NVMe acceptance requirement.
- **rev 40 (2026-09-29, controlled F1 backend comparison):**
  - A same-run three-fork comparison used the same `Binary.iterateEntityData` adapter and visitor
    callback for both scanner backends. VarHandle measured 8.475 M vs NIO 8.075 M ops/s at 64 KiB
    (+4.9%), and 399.3 k vs 400.2 k at 1 MiB (-0.2%). Against Serializer raw, the selected path
    measured 4.2% slower at 64 KiB and 10.1% slower at 1 MiB. Keep VarHandle for its measured
    small-scan win; the long 1 MiB/Linux acceptance gate remains open.
- **rev 41 (2026-09-29, D-09 pending COMMIT path):**
  - Store commits now await recording of prepare data before local persistence, offer COMMIT
    without waiting for its Archive recording, and suspend admission if the bounded offer fails.
    The maintenance task retries the marker; shutdown leaves the durable Store mark for A1.5
    recovery instead of writing a contradictory ABORT.
  - The facade recognizes `ReplicationPendingException` through bounded, allocation-free cause
    traversal, including Store wrappers. Mixed replication failures, errors, cycles, and excessive
    depth still latch the graph. `GuardingStorageManagerRejectionTest` passes; focused coordinator,
    publisher, transport-gate, and monitoring tests passed in the preceding run. Full A1 acceptance
    remains open.
- **rev 42 (2026-09-29, pending-commit and fatal-state follow-up):**
  - Added cause-chain regressions for wrapped pending commits, cycles, excessive depth, and unrelated
    replication failures. Focused facade/coordinator/publisher/gate/monitoring tests pass.
  - Fatal coordinator errors now remain visible through `failure()` as a typed replication failure
    with the original `Error` retained as its cause; the publisher was already closed, but the
    empty coordinator failure could have made node-level status report `LIVE`.
- **rev 43 (2026-09-29, D4 outcome mapping):**
  - `NodeException.outcome()` now exposes four recovery hints derived from concrete types:
    retry-safe `WriteRejectedException`, locally durable `ReplicationPendingException`,
    `ReseedRequiredException`, and fail-closed `FAILED` for all other node exceptions. A focused
    API test pins the guarantees.
- **rev 44 (2026-09-29, retention watermark ordering):**
  - Added tests proving that a later sequence with a non-advancing position and a conflicting
    position for the same sequence are rejected without replacing the accepted quorum boundary.
    `AeronArchiveRetentionTest` passes. The N3 lifecycle-scheduled two-reader acceptance remains
    open; existing two-reader integration exercises direct retention deletion.
- **rev 45 (2026-09-29, graph-facade contract docs):**
  - Documented that clean rejections and locally accepted pending commits preserve graph validity,
    and that close drains admitted application sections. Cause-chain helpers now state their
    recovery contracts.
- **rev 46 (2026-09-29, current verification ledger):**
  - The focused pending-commit, exception-outcome, and retention suites pass on this source state.
    Full unit and integration verification remains for after code changes; soak and crashmatrix stay
    in the final test stage, with soak collected under JFR and `jcmd`.
- **rev 47 (2026-09-29, no-credential transport configuration):**
  - Removed the production-required shared wire-nonce setting. The redundant framing value now
    derives from the public cluster UUID, so no secret or node-authentication credential enters
    transport configuration; encryption remains absent by requirement. The N3 scheduler-driven
    two-reader test remains open.
- **rev 48 (2026-09-29, scanner allocation pass):**
  - F1 scanners now use absolute native-order VarHandle reads on the supplied buffers and logical
    lengths, removing temporary `ByteBuffer` duplicates from full scans and non-native-order
    chunks. Focused parser/prefilter tests pass. A short local three-fork JMH run measured the
    production `Binary` type scan 3.5% faster than Serializer raw at 64 KiB and 12.2% slower at
    1 MiB; the larger-size and Linux/NVMe gates remain open. The invalid standalone-as-reader seed
    tests were removed; empty Aeron reader coverage now uses actual Aeron settings.
- **rev 49 (2026-09-29, F1 boundary correction):**
  - Corrected the F1 description: PerunCS validates the complete framing with bounded VarHandle
    reads, then Serializer's upstream materializer still receives the direct-buffer address it
    requires. A `ByteBuffer.getLong` prototype was slower on the 64 KiB gate and was discarded;
    production remains on VarHandles. JMH allocation results were within profiler noise and do not
    close the 1 MiB or Linux/NVMe gates.
- **rev 50 (2026-09-29, mandatory reader-pool ownership):**
  - Removed the unused copy-then-import adapter and all nullable-pool allocation/release paths from
    `StorageBinaryDataImporter`; deferred copies and releases now require the reader's bounded
    `NativeBufferPool`. Focused importer, buffer, and apply-worker tests pass. The Store source check
    confirms its byte-buffer import source does not free caller buffers; arena throughput and native
    leak gates remain open.
- **rev 51 (2026-09-29, native-memory boundary):**
  - Added one internal `NativeMemory` facade and routed `NativeBufferPool`, receiver fallback, and
    writer staging allocation/release through it. Focused pool, importer, publisher, and coordinator
    tests pass. The facade still uses Serializer `XMemory`; Arena adoption remains gated on the F2
    throughput and native-memory tests.
- **rev 52 (2026-09-29, F2 ownership gate):**
  - The real Store import integration now also imports shared-Arena-backed direct views, shuts down
    Store before closing the Arena, and passes. This closes F2 spike gate 1 for `importData`; F2
    throughput comparison, native-memory tracking, and arena-backed N1 stress remain open.
- **rev 53 (2026-09-29, focused re-verification):**
  - Re-ran the focused C1/C2/C3/A4/F1/N3 unit suites against the current tree; all passed, including
    writer clean-rejection/recovery, application-section drain, starter-backup timeout, backup
    single-flight, real Serializer header equivalence/fuzz, and retention watermark cases. The
    production lifecycle-drain integration test and all full acceptance profiles remain open.
- **rev 54 (2026-09-29, drain ownership cleanup):**
  - Extracted the application-section admission counter and bounded drain from the Store facade into
    package-private `ApplicationSections`. The existing drain/rejection tests pass after extraction;
    lifecycle integration and full-profile execution remain open.
- **rev 55 (2026-09-29, status API and allocation cleanup):**
  - Deleted the now-unused internal `ReplicationMetrics` record and its duplicate lag/monitoring
    tests. `StorageNodeControl` now returns the public `ReplicationStatus` directly; the node facade
    samples the writer sequence once, and the status is one flat record with primitive `-1`
    sentinels plus `has…()` accessors. Removed metric defaults from `StorageNodeHealthCheck` and
    `StorageNodeControl`. The explicit NOT_CONFIGURED status and unknown-boundary behavior are
    covered by focused tests, which pass. A volatile duplicate snapshot and an apply-time timestamp
    are intentionally omitted: status is built once per API read, and no current apply event owns
    that timestamp.
- **rev 56 (2026-09-29, API-to-node boundary):**
  - Changed `ClusterNode` to an API-only interface and moved its lifecycle implementation into the
    internal node package. `ClusterStorage` is now the only API-package entry that imports node
    assembly; the immutable foundation delegates through it. The role-accessor test fixture now
    selects Aeron explicitly, so it tests wrong-role rejection rather than standalone-role
    resolution. Package and focused lifecycle/status tests pass.
- **rev 57 (2026-09-29, drain spec synchronization):**
  - Updated C2 to describe the package-private top-level `ApplicationSections` helper actually used
    by the guarded Store facade. Its admission check uses `NodeClose.checkOpen()` under the section
    lock; lifecycle drain rejects an unexpected manager instead of silently proceeding. The
    production close-sequencer integration test exists; final integration execution remains open.
- **rev 58 (2026-09-29, single-implementation cleanup):**
  - Replaced the graph-update SPI with `Consumer<Runnable>` and wire production directly to
    `StorageGraphCoordinator.write`; removed its holder and factory. Collapsed `StorageUsageGauge`'s
    lone implementation into one final class and shared the recursive scan scratch across
    directories. Existing coordinator and gauge tests remain for the final run.
- **rev 59 (2026-09-29, disk-usage call-site cleanup):**
  - Storage and backup managers now depend on the standard-library `LongSupplier` for the cached
    disk-size snapshot instead of carrying the gauge type. Production passes the gauge's existing
    snapshot method reference; test fixtures use constant suppliers. Production and test sources
    compile with `mvn -q -DskipTests -DskipITs package`; execution of all test profiles remains
    pending.
- **rev 60 (2026-09-29, hot-path constructor cleanup):**
  - Grouped transaction-assembler and publisher construction state into immutable records built
    once per reader/publisher. `EnvelopeFramer` reuses its publisher configuration, and `Delivery`
    keeps its existing reusable mutable holder with a five-argument commit path so the hot path does
    not allocate a per-transaction record. Production and test sources compile; runtime verification
    remains pending.
- **rev 61 (2026-09-29, node-local Aeron paths):**
  - The MediaDriver now defaults to `<PERUNCS_STORAGE_PATH>/aeron`, beside the Store directory;
    Store, driver, and Archive directories are rejected when they overlap. README defaults match.
    `AeronSettingsTest` passes this default and overlap check.
- **rev 62 (2026-09-29, Aeron poll backpressure):**
  - Merger-owned readers check a volatile resident-byte snapshot before transferring a completed
    binary from the fragment callback. If admission would wait, the assembler retains the same
    buffers, breaks the poll, and retries after `controlledPoll` returns. The steady-state path
    adds no queue lock or per-transaction allocation. `ApplyQueueCounterTest` and
    `TransactionAssemblerBarrierTest` pass.
- **rev 63 (2026-09-29, retention maintenance pass):**
  - The writer lifecycle now schedules the transport's shared `maintainRetention()` operation.
    The real two-reader integration fixture schedules that same operation while one reader lags
    and after both readers advance; the generic maintenance scheduler's failure/recovery behavior
    is tested separately. Final integration execution remains pending.
- **rev 64 (2026-09-29, replication-mark dictionary check):**
  - Real reader integration checks the registered `ReplicationMark` type in the reader's persisted
    type dictionary before its first import, covering S-4's pre-import requirement. Integration
    execution remains pending.
- **rev 65 (2026-09-29, F1 native-order fast path):**
  - The bounds-checked scanner now uses absolute `ByteBuffer.getLong` reads for Serializer's
    native-order buffers and retains the VarHandle read only for a non-native-order view. It does
    not mutate caller order or allocate a duplicate view. The writer and reader scanner tests now
    cover both buffer-order cases.
  - A corrected Java 27 JMH comparison now measures both type and object-id work from the writer
    commit path. It measured the checked scan about 29% slower than Serializer raw at 64 KiB and
    10% slower at 1 MiB, so the F1 performance gate is **open and failing**. The owner deferred
    further benchmark work on 2026-09-29; Linux/NVMe execution is waived. Historical baseline
    commit `9975d95` evidence remains absent.
- **rev 66 (2026-09-29, review follow-up):**
  - Corrected the F1 performance status to use the writer's combined type/object-id scan rather
    than the earlier type-only measurement. Further scanner optimization and the F1 benchmark gate
    are deferred at the owner's request; the remaining OPUS implementation work continues.
  - Removed `NodeSettingsSource`'s redundant `LazyHolder`: assembly reads the provider immediately
    to resolve the graph timeout and role, so it is now a direct immutable reference. D1's typed
    `NodeConfig` replacement remains open.
  - Restored `NodeCollaborators.getReplicationLogRetention()` to private visibility after the
    lifecycle refactor left it used only by its owning class.
  - Centralized the remaining Aeron environment key strings in `NodeSettingsSource.EnvKeys` and
    updated `AeronSettings` to consume those constants. The parser/default `Setting` enum and typed
    `NodeConfig` migration remain open.
- **rev 67 (2026-09-29, writer header scan):**
  - The combined writer scan now reads object IDs only for entities whose type is the registered
    `ReplicationMark`; all entity type IDs still feed the index pre-filter. If the mark type is
    absent from a non-null dictionary, the existing full scan remains so missing-mark rejection
    and index detection retain their prior behavior. `ClusterIndexCommitPrefilterTest` checks that
    an object-id match on a non-mark type is ignored; the regression test is added but not run.
    F1's measured performance miss remains deferred and has not been remeasured.
- **rev 68 (2026-09-29, starter backup diagnostics):**
  - Startup now reports timeout and cancellation as distinct bounded-backup failures while retaining
    the uploaded Store image. The focused message assertions are added but not run; production
    startup-path integration coverage remains open.
- **rev 69 (2026-09-29, A1.5 recovery decisions):**
  - Split recovery-tail replay I/O from the bounded decision scanner. New focused tests exercise
    recording/window bounds and A1.5's identity, complete-prepare, terminal-marker, and sequence
    decisions through the same scanner used by production. They are not run yet; replay-through-real
    Archive and process-crash acceptance remains open.
- **rev 70 (2026-09-29, backup publication lock):**
  - Removed the JVM-global per-volume mutex registry. The advisory file lock now serializes both
    same-process and cross-process publication/deletion; same-process overlapping `tryLock()` calls
    retry within the existing bounded timeout. The existing concurrent same-name publication test
    covers the lock contract. This closes SEC5's runtime-state concern; immutable metadata caches and
    scoped test hooks remain static by design.
- **rev 71 (2026-09-29, A1 terminology cleanup):**
  - Updated smoke/integration test names and storage-merger docs that still described the deleted
    atomic cursor files. Reader restart authority is the committed Store mark; in-memory progress
    remains a `ReplicationPosition`.
- **rev 72 (2026-09-29, A1 terminology follow-up):**
  - Removed remaining test and parser comments that described a persisted reader cursor. They now
    identify the Store mark as the persisted fencing/resume authority and `ReplicationPosition` as
    the in-memory progress value.
- **rev 73 (2026-09-29, storage-limit seam):**
  - Deleted the one-method `StorageSizeValidation` interface and passed the standard-library
    `BooleanSupplier` directly to the guarded Store facade. Production still reads the same atomic
    limit snapshot; tests use the same small lambda.
- **rev 74 (2026-09-29, Aeron settings consistency):**
  - Aeron threading defaults now reuse the production-mode value already parsed for the same settings
    snapshot, instead of reading a potentially changing provider a second time.
- **rev 75 (2026-09-29, node manager simplification):**
  - Folded the sole `StorageNodeManager.Default` implementation into the package-private manager
    class, removing a one-implementation interface and its copy-through factory wrapper.
- **rev 76 (2026-09-29, production import cleanup):**
  - Replaced the remaining fully-qualified production references with imports. The code-source sweep
    found only a package-prefix string used for classpath policy, not another type reference.
- **rev 77 (2026-09-29, health-check simplification):**
  - Folded the sole health-check implementation into `StorageNodeHealthCheck` and inlined its
    one-use storage-readiness helper.
- **rev 78 (2026-09-29, atomic storage-limit state):**
  - Replaced `StorageLimitGate`'s monitor-protected pair of atomics with one CAS-managed state word.
    Readers now observe the measurement-known and limit-reached bits together, and updates no
    longer acquire a monitor. Existing hysteresis tests cover the threshold transitions; final
    execution remains pending. The production `synchronized` count is now 130 source lines.
  - Test-source import cleanup began with direct `java.*` and project-type references in touched
    suites. Production remains free of fully-qualified type expressions; further test-only cleanup
    is cosmetic and remains open.
- **rev 79 (2026-09-29, lifecycle interface removal):**
  - Deleted the one-implementation `NodeAssembly` interface and folded its builder and startup
    bridge into `NodeLifecycle`; the lifecycle test now uses the `NodeLifecycleTest` name.
    `NodeCollaborators` now has its own source file. The lifecycle
    still owns startup, close ordering, and manager/control views; this removes the extra type
    boundary without changing the public `api` package.
- **rev 80 (2026-09-29, health-check close state):**
  - Replaced `StorageNodeHealthCheck.close()`'s monitor with one `AtomicBoolean` transition. This
    keeps replication-health disposal one-shot while removing a monitor from the shutdown path.
- **rev 81 (2026-09-29, storage-check admission):**
  - Replaced `StorageTaskExecutor.runChecks()`'s monitor with a CAS-installed `FutureTask` and an
    atomic shutdown sentinel, keeping one check active while making shutdown and submission share
    one ordering point. The focused test now races two submissions behind a latch and waits for
    completion without polling. The source now contains 128 `synchronized` lines; the remaining J2
    audit is open. Tests and builds remain deferred until the code sweep is complete.
- **rev 82 (2026-09-29, reader seed gate):**
  - Removed the second reader seed check after transport collaborators were created. One fail-closed
    check now runs immediately after restore and before publisher/transport creation, and it also
    rejects a restore policy that reported success without leaving a Store image. Existing seed
    tests cover the empty-reader and missing-mark cases; execution remains deferred.
- **rev 83 (2026-09-29, test type references):**
  - Replaced several direct fully-qualified PerunCS type references in tests with imports.
- **rev 84 (2026-09-29, complete test type scan):**
  - Expanded the scan to include types directly under `peruncs.cluster.<package>`, which the earlier
    scan pattern missed. Replaced the remaining 31 occurrences across eight test suites, fixed a
    stale close-test description, and verified zero fully-qualified PerunCS type references remain
    in test bodies. Test execution remains deferred.
- **rev 85 (2026-09-29, manager close ownership):**
  - Removed `StorageNodeManager`'s close monitor and redundant `closed` flag. Its only production
    caller is the lifecycle's serialized close sequencer; per-resource completion flags retain
    retry behavior. The production `synchronized` count is now 127 source lines.
- **rev 86 (2026-09-29, backup manager close retry):**
  - Removed `BackupNodeManager`'s monitor and redundant `closed` flag. The lifecycle serializes the
    only production close caller, while `StorageBackupTaskExecutor` already tracks and retries its
    own bounded shutdown. A manager retry now always reaches that executor instead of returning
    after client disposal. The production `synchronized` count is 126 source lines.
- **rev 87 (2026-09-29, storage manager role type):**
  - Deleted `StorageNodeManager.Role` and use the shared `NodeRole`, preserving standalone's
    write-capable behavior while rejecting `BACKUP_READER` (which has its own manager). The role
    test now covers both cases; execution remains deferred.
- **rev 88 (2026-09-29, guarded write-section deduplication):**
  - Routed persistence writes, `GraphBoundary.write`, and root replacement through one shared
    admission/validation/exclusive-section helper. Persistence retains its retryable-rejection and
    uncertain-failure latch policy; arbitrary application writes and the in-memory root swap keep
    their distinct failure contracts. No tests were run during this code sweep.
- **rev 89 (2026-09-29, current test-map names):**
  - Updated the C2 status and step-1 test map to the renamed `NodeLifecycleTest`; the old
    `NodeAssemblyLifecycleTest` no longer exists. C2 integration execution remains pending.
- **rev 90 (2026-09-29, backup file modes):**
  - Create completed backup ZIPs with owner-only POSIX permissions before writing, so the mode
    remains `0600` when the archive is atomically published. The export workspace remains `0700`
    where supported; non-POSIX filesystems keep their native defaults. Added assertions for both.
- **rev 91 (2026-09-29, persistence write allocation):**
  - Routed void persistence writes through direct void write-section and app-section overloads,
    removing both `Supplier` adapters from the write path. Admission, validation, and the
    retry-versus-latch classification remain shared. The rejection test now exercises both the
    result and void persistence paths and confirms application-boundary failures remain
    caller-managed. Tests remain deferred until final verification.
- **rev 92 (2026-09-29, module descriptor documentation):**
  - Reduced the module descriptor's architecture essay from 162 lines to a concise boundary note.
    It retains the fixed one-writer topology, local live-storage rule, embedded-index rule, graph
    invalidation contract, and explicit no-authentication/no-encryption requirement.
- **rev 93 (2026-09-29, settings snapshot consistency):**
  - Parse and validate the Aeron driver timeout once per settings snapshot, then reuse it in the
    Archive and operation-timeout configuration. Added a read-count regression check. The typed
    `NodeConfig` and `Setting` enum migration remains open.
- **rev 94 (2026-09-29, readiness allocation):**
  - Maintenance health lookup now returns the first degraded task with a direct map iteration,
    avoiding a temporary Stream pipeline on each readiness/health probe. The benchmark gate stays
    deferred by the owner.
- **rev 95 (2026-09-29, storage-check shutdown):**
  - Removed `StorageTaskExecutor`'s close monitor and separate `closed` flag. Its atomic shutdown
    sentinel already rejects new checks, and `ExecutorService.shutdownNow()` plus bounded termination
    waits are safe to repeat, including concurrently. J2's production synchronization audit remains
    open; this reduces the source count from 127 to 126 lines. Tests remain deferred until the final
    verification phase.
- **rev 96 (2026-09-29, environment snapshot):**
  - `NodeSettingsSource.env()` now snapshots `System.getenv()` once when the provider is created.
    Every setting read sees the same map, with no repeated environment lookup or nullable-source
    branch. This is an incremental D1 cleanup; the typed `NodeConfig` migration remains open.
- **rev 97 (2026-09-29, typed settings and user documentation):**
  - Replaced the settings SPI with `NodeConfig`; Aeron and merger limits now use its immutable
    typed values. Removed the retired durability-mode setting, reject Aeron-specific values when
    transport is `none`, and validate that the merger backpressure threshold does not exceed its
    hard cap. `NodeConfig.settingsMarkdown()` now matches the complete README table, with an
    exhaustive per-setting test. Removed the unused derived `refreshBudget` field; the merger owns
    its combined apply/index-refresh deadline. Added the Eclipse Store porting map and framework-
    neutral container lifecycle and health guidance. Test execution remains pending.
- **rev 98 (2026-09-29, maintenance bounds):**
  - C12 retry counts and maintenance close bounds now come from typed `NodeConfig.Operations`,
    threaded into backup publication, backup stop/retention, maintenance, and storage-check
    executors. Removed `NativeBufferPool`'s duplicate retention ceiling, which was already supplied
    by `NodeConfig`. README defaults and an override-parsing test are updated; execution is pending.
- **rev 99 (2026-09-29, compile sweep):**
  - Fixed the module descriptor's invalid import and bare `@since` annotation, refreshed merger
    configuration call sites and test settings maps, and fixed the watermark test's byte-order
    import. Production and test sources compile with Java 27; test execution remains pending.
  - Confirmed the S-4 pre-import dictionary assertion is present in `AeronStoreIntegrationIT`,
    `AeronArchiveReader` has explicit stop/reconnect/disposal state transitions with lifecycle
    coverage, and the existing focused internal packages all carry package documentation. D9 is
    recorded against those narrower package boundaries; only `api` and `errors` are exported.
- **rev 100 (2026-09-29, crash-hook consolidation):**
  - Replaced the publisher, atomic-file, and backup backend's separate scoped hooks with one
    `FaultInjection` seam. Crash tests bind it directly; explicitly started threads inherit the
    binding, and production calls allocate nothing when no hook is bound. Deleted the unused
    reader crash-hook bridge. Removed `AtomicFileWriter`'s ineffective hook bypass branch; test
    compilation passes after consolidating all phases through the shared seam.
- **rev 101 (2026-09-29, remaining-item review):**
  - Audited the remaining monitor-protected state and the proposed assembly, codec, and structured-
    concurrency changes. Kept synchronization where it protects coupled lifecycle/transaction state;
    withdrew role-specific assembly classes, a generic record codec, and `StructuredTaskScope` fan-out
    because they add machinery without an independent role path, shared wire schema, or parallel
    lifecycle work. Reader-thread consolidation and writer gather-offer/CRC changes remain deferred
    until performance evidence can compare them with the current paths.
- **rev 102 (2026-09-29, status reconciliation):**
  - Reconciled C5/A5 with the current design: `LazyHolder` synchronization stays for the
    in-flight-factory close race, and separate role assembly classes are withdrawn. The starter-
    backup helper test covers the production wait/delete boundary; full startup-path proof remains
    an acceptance gap. Benchmark and 5% gates remain deferred by the owner. PerunCS still has no
    node authentication or transport encryption.
- **rev 103 (2026-09-29, C7 API inventory):**
  - Added `GateClassificationTest`, a test-only reflection inventory of the public Eclipse Store
    interfaces. Unknown method names fail the test; existing gating suites continue to exercise
    behavior. No production reflection was added. The focused inventory test passes; final suite
    execution remains pending.
- **rev 104 (2026-09-29, production reflection boundary):**
  - Centralized all production `java.lang.reflect` use in package-private `StoreIndexReflection`;
    no `Field` escapes that boundary. This does not clear the code: only J1-a's minimal JVector
    invalidation bridge is accepted temporarily, until PR #832 lands. J1-b's group enumeration,
    Lucene-context access, and object-reference walk remain production-reflection red flags and must
    be removed or redesigned against supported Store/Serializer APIs. PR #832 does not resolve them.
    No further production reflection may be added. The C7 reflection inventory remains test-only.
- **rev 105 (2026-09-29, remove redundant vector reflection):**
  - Removed the post-search read of Store's private `graphRebuilt` flag. The warm-up uses the public
    `VectorIndex.search()` lazy-initialization contract; the PR #832 invalidation bridge remains the
    only temporary vector reflection. J1-b's other production reads remain open red-flag debt.
- **rev 106 (2026-09-29, forked writer restart fixture):**
  - The process-restart test now loads the Store mark before initializing the writer, routes each
    direct Store write through the mark-aware helper, and injects rejection after startup fencing.
    It compares the failed write with that process's own pre-write boundary and explicitly fails if
    a retry succeeds. The focused forked restart test passes; the final profiles remain pending.
- **rev 107 (2026-09-29, remove non-vector production reflection):**
  - Replaced reflective group enumeration, Lucene-context reads, and object-field traversal with
    Serializer's `PersistenceTypeHandler.iterateInstanceReferences` and runtime type descriptions.
    The type-handler manager now flows from Store startup into writer validation and the apply worker.
    `java.lang.reflect` remains in production only for JVector invalidation while PR #832 is open.
    Focused index-validation, prefilter, scratch, and writer-validation tests pass; full gates remain
    pending. The 5% scan benchmark stays deferred by the owner.
- **Current review follow-up (2026-09-30):**
  - Re-audited production sources: reflective field discovery exists only in
    `StoreIndexReflection`, called solely for temporary JVector invalidation. Test-only reflection
    inventories remain tests. PR #832 is still open; SEC5 names this one exception explicitly.
    Any other production reflection is red-flag debt and is prohibited; new reflective access must
    stay out of production unless the owner explicitly revises this boundary.
    `StoreIndexReflectionTest` now enforces that boundary against production source files; execution
    remains pending.
  - Vector-index validation now charges each entry to the shared scan bound and iterates the
    upstream table under its parent-map lock without copying entries. Its bound regression test is
    added; execution remains pending.
  - Full index scans detect in-place runtime-binding changes and rebuild reachability caches only
    when the dictionary changes; the writer pre-filter checks the constant-time type count and
    resets its last-type cache per commit. A late-index-type regression test is added; execution
    remains pending.
  - Reconciled P1-7's stale deletion list against the live path: `ApplyQueue` and `ApplyWorker` keep
    ownership accounting separate from Store materialization, the queue condition replaces a timer,
    and the watchdog remains to fail a callback that cannot report its own timeout. The receiver's
  ownership and non-blocking admission methods prevent duplicate frees and blocking in Aeron's
  delivery callback. This source shape is retained; final stress and suite execution remain open.
  - Added the C3 full-startup regression for preserving an uploaded Store on starter-backup failure.
    Added checked N1 lease generations, test-only buffer poisoning, and a forked NMT pool-close test;
    focused execution remains pending. Added the 10k randomized real-Store materialization stress
    fixture and wired it into `-Pintegration`; its execution remains pending.
  - The earlier A1 sweep removed old cursor/checkpoint persistence paths and the manifest
    compatibility check. The latest cleanup compiles; full-suite,
    integration, soak, and crashmatrix gates remain for the final verification step.
  - Any preparation failure followed by a successfully recorded ABORT now becomes a clean
    `WriteRejectedException` in both coordinator and low-level publisher paths. The original
    failure remains in the cause chain and writer startup resolves the recorded tail;
    cause cycles, excessive depth, errors, and unrelated replication failures still fail closed.
  - Application drain uses `NodeClose.awaitAppIdle(Duration)` and the settings timeout.
    `ApplicationSections` uses a lock-protected count; lifecycle admission under that lock is the close
    gate, so a second permanent `closing` flag is unnecessary.
  - The direct low-level publisher and coordinator now assert the durable ABORT position: the
    low-level case consumes sequence 0 and then commits sequence 1, while the coordinator case
    aborts sequence 1 and then commits sequence 2. The low-level test also covers a non-timeout
    failure during data publication. A failure at `AFTER_PREPARE_BEFORE_LOCAL_WRITE` now has an
    explicit no-ABORT/fail-closed regression. These tests await the final run. The index-validation
    bound has one shared default/parser; blank values use that default and malformed or non-positive
    values fail.
  - The P1-1 integration proof now registers an external Lucene index on an already-stored GigaMap,
    checks the rejection cause and unchanged replication sequence, then verifies a plain write
    succeeds. This test edit awaits the final test run.
  - Checked N1 test mode now retains at most one native buffer across all size classes, and the
    existing pool behavior tests explicitly select production mode. `StorageBinaryPoolOwnershipIT`
    is wired into `-Pintegration` for 10k randomized real-Store transactions; `NativeMemoryTrackingTest`
    now holds 60 MiB in the pool before measuring its explicit close. Both remain unrun.
    The integration profile also keeps the default Archive and UDP replication suites while adding
    the Store, driver-failure, application-drain, and ownership-stress suites.
  - Replaced retry jitter's `ThreadLocalRandom` with one shared `Random`; retries are the cold path,
    and this keeps production code within AGENTS.md's no-ThreadLocal rule.
  - Removed the unread provider error code from `ReplicationUnavailableException`; the original
    Aeron exception remains the cause. `ReplicationPositionUnavailableException` is now in the
    unexported `errors.internal` package, and the node/replication hierarchy is sealed with final
    leaves. `ReplicationPendingException` is now defined by A1; D4's four-way `outcome()` mapping
    is covered by `NodeExceptionOutcomeTest`. These source and test edits await the final test run.
  - S-4's explicit assertion that the mark type is present in the reader dictionary before the
    first post-seed import is present in `AeronStoreIntegrationIT`; integration execution remains
    open. The production mark is also used by the 200-trial atomicity crash test.
  - Starter backup waits are bounded. Retention caps requests at the complete reader-quorum
    watermark, preserves history for incomplete or lagging readers, and has a configurable cadence.
  - C5 deliberately keeps `LazyHolder` synchronization: close must wait for a resource factory
    already in progress before it can skip disposal; `volatile initialized` alone loses that race.
    A5's separate role-assembly classes are withdrawn: current role differences are a few explicit
    branches, and separate graphs would duplicate shared lazy-resource and close semantics.
  - A focused backup-executor audit confirms source coverage for queued-close late entry, bounded
    close while running, runtime submit rejection, fatal submit failure, and callback execution
    outside the state lock. These cases are still awaiting the final test run.
  - Publisher, atomic-file, and backup crash points now share one `FaultInjection` scoped hook;
    obsolete subsystem-specific hook wrappers and the unused reader bridge are deleted. Production
    fault points remain allocation-free when unbound. Production and test sources compile.
  - J1-a's interim vector teardown now routes through `StoreIndexReflection.invalidateVectorGraph`,
    leaving one call site for adopting upstream `VectorIndex.invalidateGraph()`. Vector graph
    warmup now runs after the coordinator write side, under its read side; both changes await the
    final test run. The upstream PR remains open.
  - All production `Field` discovery and offset reads are now confined to `StoreIndexReflection`;
    index validation now uses Serializer's supported handlers. J1-b is implemented; J1-a waits for
    upstream PR #832. The sole reflection bridge remains a temporary exception, not a pattern for
    other production code.
  - `mvn -q -DskipTests compile` and focused `ClusterStoreIndexesTest`,
    `StoreIndexReflectionTest`, and `GateClassificationTest` pass after this boundary cleanup.
    Full unit, integration, crashmatrix, and soak verification remains pending.
- **rev 108 (2026-09-30, real-Archive writer crash recovery):**
  - Added a forked writer-restart test for crashes after recorded PREPARE but before local Store
    write, and after local Store write but before COMMIT. It verifies recovered ABORT/COMMIT in the
    live Archive, Store presence, and a successful next commit. The test is wired into
    `-Pcrashmatrix`; execution remains pending until final verification.
- **rev 109 (2026-09-30, native allocation lifetime and writer schema):**
  - Replaced PerunCS-owned direct-buffer allocation with shared FFM arenas. The reader pool keeps
    each arena with its buffer, reuses retained buffers, and closes an arena on eviction or pool
    shutdown. This is needed because the configured Java 27 Serializer memory accessor reports
    `deallocateDirectByteBuffer()` unsupported; the prior pool therefore retained native memory
    after explicit release. The forked NMT test now verifies the `Other` committed-memory drop.
  - The writer now sends a full type-dictionary snapshot only when its append-only type count
    changes. This covers Store-created internal types such as `GigaLevel2` when Store does not
    invoke the wrapped exporter before the corresponding data commit.
  - Focused merger ownership, NMT, Store import, writer smoke, and monitoring tests pass. Full
    integration, crashmatrix, and soak runs remain pending; the deferred F1/F2 performance gates
    remain open.
- **rev 110 (2026-09-30, final implementation sweep and verification):**
  - The soak rollback fixture now quarantines a reader after rewinding only its replication mark;
    the next seed restores a consistent Store snapshot. Reader Archive recording discovery is
    cached for the lifetime of its transport, avoiding repeated alias lookups after a reader
    transport restart.
  - Final Java 27 verification passed: `mvn verify -Pintegration` (782 unit tests, 1 skipped;
    26 integration tests), `mvn verify -Pcrashmatrix` (782 unit tests, 1 skipped; 17 crash and
    integration tests), and `mvn verify -Psoak` (782 unit tests, 1 skipped; soak passed).
    The soak reported 568 transactions, 42,489 verified reads, zero torn transactions, and
    successful restart/reseed/rejoin coverage. A subsequent soak-only JFR/jcmd run also passed.
  - Fresh JFR: 171,457 events over 83 seconds, 27 ms maximum GC pause, zero pauses over 100 ms,
    zero monitor-block time, zero unknown-duration/metadata events, and zero virtual-thread pins.
    Live `jcmd` reported about 120 MB heap used and no deadlock.
  - Baseline and 5% performance gates remain explicitly deferred by the owner. PR #832 remains
    open; `StoreIndexReflection` is the sole production reflection boundary, limited to temporary
    JVector graph invalidation until upstream exposes the supported API. No node authentication
    or transport encryption is required or permitted.
- **Performance follow-up (2026-09-28):**
  - Same-run Java 27 JMH comparison of native-order `ByteBuffer`, FFM, VarHandle, and Serializer's
    raw iterator selected `ByteBuffer`: 6.73 M ops/s at 64 KiB versus 6.27 M for FFM and 6.66 M
    for VarHandle. At 1 MiB it measured 330 k versus 339 k for FFM and 334 k for VarHandle; the
    confidence intervals overlap. The simpler NIO path wins on the smaller scan. Its raw-iterator
    gap still exceeds the 5% gate, which remains open.
  - The writer pre-filter now reads only type ids. A same-work JMH comparison measured checked/raw
    at 8.027/8.684 M ops/s for 64 KiB and 340.9/437.1 k ops/s for 1 MiB. The raw/checked ratios
    (1.082 and 1.282) still fail the 1.05 gate; the short 64 KiB run had high raw-side variance.
- **rev 5:**
  - Data Grid-derived items removed: the `ClusterFoundation` naming, the exported
    `ObjectGraphUpdateHandler` SPI and the provider SPI. A9 is now purely "Eclipse Store API
    compatibility".
  - C10 corrected: the rev-3 fix would have wedged close.
  - C1 precedence and default stated; C2 admission mechanics stated.
  - A1: spike S-0 now answered from the Aeron source; ordered recovery table; definitive
    delete/edit lists; external-Archive decision.
  - A8 rewritten: discovery already exists.
  - C7 rows completed; C9 type specified.
  - Per-step acceptance criteria, test-to-step mapping and ID tombstones added.
  - The "run a formatter" instruction dropped.

---

## 1. Decision log

| # | Topic | Decision | Source |
|---|-------|----------|--------|
| D-00 | Scope | As in §0. | Owner |
| D-01 | Roles and failover | Fixed roles (a product invariant, `module-info` "Fixed roles"). The library chooses no writer and performs no failover. It guarantees that one Store cannot have two writers, and readers fail closed on stale writers. | Owner |
| D-02 | API | Eclipse Store API compatibility: `ClusterStorageManager<T> extends StorageManager` (exists, `M/api/ClusterStorageManager.java:41`) plus a `ClusterStorage` entry point mirroring `EmbeddedStorage` (A9). No Data Grid SPI or names. | Owner |
| D-03 | A1 | Do it, gated by the benchmark (D-25). | Owner |
| D-04 | Writer exclusivity | Local `writer.lock` + a token in the Store mark (A2). The NFS lease is deleted. Two hosts sharing one Store path is unsupported operator error, documented. | Owner (D-01) |
| D-05 | C1 | An exported `WriteRejectedException` is thrown at named sites. The classifier default is **latch**; uncertain wins over clean. No message matching. | Default |
| D-06 | C2 | Facade-level `ApplicationSections` counter (outermost section per thread), extracted as a package-private store helper. Admission reuses the lifecycle's existing closing flag. It is permanent, not interim. | Default |
| D-07 | C3 | `CompletableFuture<BackupInfo> createBackup(BackupSlot)`; the pause happens at task start. | Default |
| D-08 | Mark placement | Named root `"peruncs.replication"`; fallback per spike S-1. | Default |
| D-09 | COMMIT offer fails after a local commit | `ReplicationPendingException`, graph valid, admission suspended until re-offered. | Default |
| D-10 | Entry point | `ClusterStorage` (static) + `ClusterStorageFoundation<T>` (builder). Delete `ClusterNode.open(NodeOptions)` and `NodeOptions`. | Default |
| D-11 | Graph coordination API | `GraphBoundary` is the only coordination API. The internal merger accepts `Consumer<Runnable>` and production wires it directly to `StorageGraphCoordinator.write`; no graph-update handler SPI remains. | Default (rev-5 scope) |
| D-13 | Framework integrations | Not part of the library. The API is shaped so that any integration is a few lines (§5 integration guidance, docs only). | Owner |
| D-14 | Unsectioned reader traversal | Documented rule: on readers, traverse inside `graphBoundary().read(...)`. | Default |
| D-15 | Environment prefix | `PERUNCS_`. | Default |
| D-16 | Optional values in public records | AGENTS rule 3: primitive + `-1`/`""` sentinel + `has…()` accessors. `BackupInfo`, `BackupStatus`, `ReplicationApplier.StopResult`, and `ReplicationStatus` expose these accessors. | AGENTS.md |
| D-17 | Pause/resume replication API | Deleted (`BackupNodeControl.stopReadingAtLatestMessage/resumeReading/isReading`); backup work owns its own boundary pause. | Default |
| D-18 | Standalone `status()` | Returns `NOT_CONFIGURED`, never `WrongRoleException`. | README contract |
| D-19 | Roles | Exported `enum NodeRole { STANDALONE, WRITER, READER, BACKUP_READER }`. `NodeStatus.writer` becomes `NodeStatus.role()`. | Default |
| D-20 | P1-4 | Off-lock warm-up is implemented under the coordinator read side. Upstream `VectorIndex.invalidateGraph()` remains the planned replacement for the interim reflective invalidation (PR #832). | Owner + upstream |
| D-21 | A8 | Discovery **already exists**. The remaining work is an epoch in the alias, verification against the mark, and README fixes. Needed only for A2b. | Code evidence |
| D-22 | S1 | Keep same-name identity checks: they prevent a conflicting retry from overwriting a durable archive. Retain off-lock digest inspection with a destination-stamp recheck to keep large-archive hashing outside the shared lock. | Code evidence; data-loss prevention |
| D-23 | Group commit | Out of scope. | Spec |
| D-24 | Apply thread | Platform daemon `peruncs-apply`, landing in the same change as N1. | Default |
| D-25 | Benchmark gate | Writer p99 commit ≤ baseline × 1.10; writer commits/s ≥ baseline × 0.95; reader apply p99 ≤ baseline × 1.10; live end-to-end p99 ≤ baseline. | Default |
| D-26 | External Archive mode (`PERUNCS_AERON_EXTERNAL_ARCHIVE`) | **Removed.** A1's durability premise holds only for the embedded Archive, whose `fileSyncLevel` PerunCS controls (§3.A1 S-0). This also deletes the control-session fallback in `awaitRecorded`. | Default |
| D-27 | Buffer pool size classes (N1) | Keep the existing power-of-two classes (`M/storage/binary/NativeBufferPool.java`). No new scheme. | Default |
| D-29 | Off-heap memory policy (the old AGENTS rule 31 is deleted) | **Limited FFM (`java.lang.foreign`) adoption.** Agrona stays at the Aeron boundary: envelope encode/decode and offers work zero-copy on Aeron's `DirectBuffer`s. Serializer `ByteBuffer`/`Binary` stays at the Store boundary. F1 uses bounds-checked absolute `ByteBuffer.getLong` on native-order inputs and a VarHandle fallback for non-native-order views; it leaves caller state alone and creates no duplicate views. The corrected Java 27 writer-header scan missed the local 5% comparison by about 29% at 64 KiB and 10% at 1 MiB; further benchmark work is deferred by the owner, and Linux/NVMe execution is waived. The historical `9975d95` baseline artifact is still missing. FFM is reserved for native memory PerunCS allocates and frees itself (F2). FFM is final since Java 22, so no preview flag is needed. Only `Arena` allocation and `ValueLayout` access are allowed: **no restricted methods** (`ofAddress`, `reinterpret`), so `--enable-native-access` is never required. The `--add-exports java.base/jdk.internal.misc` requirement comes from Serializer's `XMemory` and its materializer iterator, which still consumes a direct-buffer address after F1 framing validation; migrating `XMemory` to FFM is proposed upstream (§4.J1-b). | Owner + default |
| D-28 | Formatting | No repo-wide formatter run (there is no formatter plugin in `pom.xml`). Fix indentation only in touched files. | Default |

**Retired IDs** (kept so references resolve):
- **A10** → C7;
- **C11** → subsumed by A2;
- **C13** → merged into C1;
- **P1-2** → resolved by A1 + A2;
- **P1-3** → subsumed by A2;
- **P1-5** → resolved by A1 (reader durability gate deleted);
- **P1-9** → deleted by A1 (no reader cursor file);
- **D-12** → dropped (no provider SPI).

---

## 2. Linearized plan

| Step | Work | Needs | Done when | Status (2026-09-30) |
|------|------|-------|-----------|--------------------|
| 0 | **Baseline benchmark** on `9975d95` (harness in §3.A1 tests). | – | `bench/results/9975d95.json` committed. | **OPEN** — baseline evidence absent. |
| 1 | Batch 1: **C1**, **C2**, **C3**, **C5**, **C6**, **C10**, **A4**. | 0 | `mvn verify` green; all §9 rows tagged step 1 pass. | **DONE** — final integration and crashmatrix profiles pass. C1 covers recorded-ABORT recovery, S+2, and fail-closed handling after local-write uncertainty. C2 covers production close/drain, timeout with Store/transport left open, retry, and restart. C3 covers bounded starter backup failure while retaining the upload; C5 keeps synchronization for the factory/close race. C10 covers late queued entry, bounded running close, submission rejection/fatal failure, and callback ordering. A4's named-module consumer and writer-reader Lucene/JVector replication coverage pass. |
| 2 | **A1 spike** S-1…S-5 (S-0 is already answered). | 1 | Each spike criterion documented as pass/fail; D-08 or its fallback confirmed. | **DONE** — S-2's 200-trial child-kill matrix, S-4 reader-dictionary assertion, S-5 scanner matrix, and real-Archive ABORT/COMMIT crash recovery passed in the final profiles. |
| 3 | **A1** (+D-26, +C1 re-verify on the reworked publisher). | 2 | `mvn verify -Pintegration -Pcrashmatrix` green with the new oracle; §9 rows tagged step 3 pass. | **DONE** — Store marks, exact writer sequence reservation, bounded Archive-tail recovery, reader replay/resume, backup restore, and removal of legacy cursor/checkpoint/manifest paths passed integration and crash recovery. D-26's external-Archive setting/fallback is removed. |
| 4 | **Benchmark compare** against step 0. | 3 | D-25 thresholds met. Otherwise stop and report to the owner. | **OPEN** — owner deferred further benchmark work. The corrected local F1 writer-header scan missed its 5% comparison (about 29% slower at 64 KiB and 10% slower at 1 MiB); Linux/NVMe execution was waived. The historical `9975d95` artifact is absent. |
| 5 | **A2** (delete the NFS lease; `writer.lock`; bootstrap commit). | 3 | §9 A2 rows pass; README network section updated. | **DONE** — the writer lock, Store-mark token advancement, stale-token rejection, and repeated-start behavior pass. README describes the supported local writer lock and shared-storage constraint. |
| 6 | **P1-1 + F1** (per-commit type-id pre-filter on a native-order, bounds-checked entity-header scanner; temporary bound key). | 1 | §9 P1-1/F1 rows pass. | **PARTIAL** — the real writer/GigaMap prefilter path, 10k malformed-length mutations, native and non-native buffer order, framing checks, and equivalence against real Serializer output all pass. The corrected scan still misses the 5% benchmark gate (about 29% at 64 KiB and 10% at 1 MiB); further comparison is explicitly deferred by the owner. Linux/NVMe execution is waived. |
| 7 | **P1-7 + N1 + F2** (apply thread; reader-owned pool, arena-backed if the F2 spike passes; receiver ownership API deleted). | 3, 6 | §9 P1-7/N1/F2 rows pass; soak green; F2 benchmark within D-25. | **PARTIAL** — bounded reader-pool ownership, apply budgets/backpressure, 10k randomized real-Store transactions, explicit FFM arena release, and NMT release checks pass. Integration and soak profiles pass. F2 throughput acceptance remains open and deferred by the owner; the receiver boundary stays because it owns cross-package buffer lifetime/backpressure. |
| 8 | **C4** re-measure (relax the lock only on evidence). | 4, 5 | Read-latency metric recorded; decision noted. | **OPEN** — benchmark harness prerequisites remain. |
| 9 | **A3** (typed position; delete `replicationStreamName`, `ReplicationCursor`, `AeronReplicationCursor`) → **A9** (`ClusterStorage`) → **D1** (`NodeConfig`, `PERUNCS_` keys; temporary keys from steps 6–7 migrate). | 3 | §9 A9/D1 rows pass. | **DONE** — typed position, `ClusterStorage` API, immutable `NodeConfig`, one settings/default enum, and README/settings parity pass the final unit and integration profiles. |
| 10 | **C7**, **A5/S4**, **A6**, **A7**, **D9** (packages + `module-info` exports). | 9 | §9 C7 rows pass. | **DONE** — the role/API inventory, standalone status, S4 reader dictionary, module-path checks, close sequencing, and per-role integration contracts pass. A5's separate role assembly classes remain withdrawn as duplicate machinery. |
| 11 | **J1-a + P1-4** (PR #832). | PR #832 in the eclipse-store snapshot | §9 J1-a rows pass. | **PARTIAL** — interim vector invalidation remains isolated in `StoreIndexReflection.invalidateVectorGraph`; warm-up stays inside the coordinator write section. All local profiles pass. The only production reflection remains this temporary bridge pending [PR #832](https://github.com/eclipse-store/store/pull/832), still open on 2026-09-30; upstream API adoption and the 1k-versus-100k write-section duration comparison remain open. |
| 12 | **N3** (writer-driven retention; P1, may be pulled forward), cleanup: C8, C9, C12, S1, S3, S5, S6, D2–D8, D10, J1-b, J2–J6, §6 security, §7 docs. Optional: **A2b**, **A8**. | 10 | Remaining §9 rows pass. | **PARTIAL** — N3's scheduled writer retention pass and lagging/caught-up two-reader fixture pass; C9/C12, S1/S3/S6, D2–D10, J2–J5, and the §7 no-authentication/no-encryption documentation pass. Generic codec, structured-concurrency fan-out, and role-specific assembly are withdrawn. J1-b removed all non-vector production reflection; only the temporary PR #832 JVector invalidation bridge remains. Reader-thread consolidation and P1-6 gather/CRC work remain deferred; F1/F2 throughput gates remain open by owner decision. Optional A2b/A8 remain unselected. |

**Current gate status (2026-09-30).** Final Java 27 profiles pass on the current source: integration
has 782 unit tests (1 skipped) and 26 integration tests; crashmatrix has 782 unit tests (1 skipped)
and 17 crash/integration tests; soak has 782 unit tests (1 skipped) and one passing soak test. The
soak reported 568 transactions, 42,489 verified reads, zero torn transactions, and successful
reader/index convergence. A subsequent soak-only JFR/jcmd run also passed: 171,457 events over
83 seconds, 27 ms maximum GC pause, no pauses over 100 ms, no monitor-block time, no virtual-thread
pins, about 120 MB live heap, and no deadlock. The F1 fuzz/equivalence and 10k real-Store pool
ownership checks passed in the integration profile; the scheduled N3 writer/two-reader quorum test
passed as well.

Step 0 baseline evidence and the step 4 / F1 / F2 performance gates remain open by the owner's
explicit deferral; Linux/NVMe execution is waived. PR #832 remains open, so production reflection is
limited to the temporary JVector invalidation bridge in `StoreIndexReflection`. Node authentication
and transport encryption remain neither required nor permitted. Crashmatrix and soak were run after
the source changes were complete; no code changes followed those runs.

---

## 3. Specifications — step 1 and A1

### C1 — Clean pre-publication rejections must not latch the writer graph (P0)

**Problem (verified).**
- `M/node/store/GuardingStorageManager.java:430-456` (`persist`): the outer `validateState()` runs
  outside the try. Those rejections (closed, graph invalid, storage limit, reader role) are clean
  and correct.
- Inside the try, `delegate.store(...)` reaches
  `M/storage/aeron/writer/AeronStorageBinaryReplicationTarget.java:102-196`
  (`validateWriterState()` `:135`, `coordinator.prepare()` `:149`), then
  `M/storage/aeron/writer/AeronReplicationWriteCoordinator.java:423-438` (`ensureWriteAdmitted`)
  and `:496-539` (`lockWriteAdmission`).
- Every rejection there happens before any byte is published or written locally, yet `persist`
  latches it.

**Decision.** D-05.

**API/format changes.**
1. **New exported type.** `peruncs.cluster.errors.WriteRejectedException extends
   ReplicationException` (final), with constructors `(String)` and `(String, Throwable)`.
2. **Throw sites** (the throw site owns the classification; `persist` infers nothing):

   | Site | Today | Becomes |
   |------|-------|---------|
   | `ensureWriteAdmitted` | capacity: `ReplicationUnavailableException` | `WriteRejectedException` |
   | `ensureWriteAdmitted:432` | `maxTransactionBytes`: `IllegalArgumentException` | `WriteRejectedException` |
   | `lockWriteAdmission` | admission timeout / interrupt | `WriteRejectedException` (interrupt flag restored) |
   | `AeronStorageBinaryReplicationTarget.prepareWrite` | a `RuntimeException` from `validateWriterState()` | wrapped as the cause of a `WriteRejectedException` |
   | `AeronReplicationPublisher.prepareWithRecovery` (`M/storage/aeron/writer/AeronReplicationPublisher.java:324-357`) | prepare failure | A prepare failure whose ABORT was successfully **awaited recorded** becomes `WriteRejectedException` in coordinator and low-level paths. It carries the durable ABORT position and preserves the original cause. If ABORT cannot be offered or recorded, it keeps today's behaviour (fail closed, latch). |

3. **Classifier.** A package-private static `boolean isCleanRejection(Throwable)` in
   `GuardingStorageManager`:
   - walk `t, t.getCause(), …` (max depth 16, allocation-free cycle detection);
   - return **true iff** a `WriteRejectedException` is found and no `Error` or unrelated
     `ReplicationException` subtype appears. One `ReplicationUnavailableException` cause is allowed
     only below a `WriteRejectedException` carrying a recorded ABORT position (uncertain wins);
   - otherwise return **false** (latch: the fail-closed default).

   `persist`'s catch invalidates only when `!isCleanRejection(failure)`. The walk covers Eclipse
   Store's wrapping (e.g. `PersistenceExceptionTransfer`) automatically.
4. **Publisher return-to-ready** (the recorded-ABORT case), under the publisher monitor:
   - `pendingTransaction = null`, `reservedSequence = -1`;
   - `nextSequence = aborted + 1` (**sequences are consumed, never reused**);
   - `preparing = false`, `failed = false`;
   - the coordinator (`finishCommit`/`abort` paths) clears `activeWrite`, clears the buffer
     scratch, and restores `retryDictionary` from the aborted write.
   - **Checkpoint:** the existing `onAbort` callback writes `REJECTED` (valid pre-A1; deleted by A1).
5. **A2 removes `WriterFencedException`**; Store-mark token regressions are classified as corrupt replication data.
6. **Re-verification.** Step 3 (A1) reworks `AeronReplicationPublisher`; re-run the C1 tests there.

**Files.**
- new `M/errors/WriteRejectedException.java`
- `M/node/store/GuardingStorageManager.java`
- `M/storage/aeron/writer/AeronReplicationWriteCoordinator.java`
- `M/storage/aeron/writer/AeronStorageBinaryReplicationTarget.java`
- `M/storage/aeron/writer/AeronReplicationPublisher.java`
- `M/errors/package-info.java`

**Invariants.**
1. A clean rejection is only raised before `delegate.write` starts.
2. No string matching.
3. Sequences are monotonic, and ABORT consumes a sequence.

**Tests** (fault injection through the existing `CrashHook` / `ScopedValue` seams and the
coordinator's `LongPredicate writeAdmission`):
- capacity → clean, and the next write succeeds after capacity returns;
- oversize → clean;
- validator rejection → clean;
- admission timeout → clean;
- a prepare failure (back-pressure or another runtime failure) with recorded ABORT → clean, the
  next sequence is S+2, and readers resolve S+1 as aborted;
- uncertain replication failure → latched;
- failure at `AFTER_PREPARE_BEFORE_LOCAL_WRITE` → latched;
- a synthetic chain `WriteRejectedException` caused by `CorruptReplicationDataException` →
  latched (precedence).

### C2 — Drain application sections before stopping replication (P1)

**Decision.** D-06. The counter is permanent: even after A1, the Store must not close under an
application section.

**API/format changes.**
- **Counter.** Package-private `ApplicationSections` owns an `int active` protected by its
  `ReentrantLock` and an `idle` condition. `enter()` checks `NodeClose.checkOpen()` under that
  lock before incrementing; `exit()` decrements in `finally` and signals when the count reaches
  zero. `awaitIdle(Duration)` waits to the caller's deadline. The lifecycle's admission state is
  the only permanent close gate; a second `closing` flag would duplicate it.
- **Single helper.** Every facade method classified **WRITE** or **READ** in C7 runs through
  `<R> R appSection(Supplier<R>)`. That includes `graphBoundary().read/write`, `persist`,
  `Database.getObject`, `exportAdjacencyData`, and (after C7) `exportTypes`, `issueFullBackup`,
  `exportChannels` and the PM READ methods.
- **Count only the outermost section:** `enter`/`exit` run only if
  `!graphCoordinator.isHeldByCurrentThread()` at entry.
- **The hook lives at the facade layer, not in `StorageGraphCoordinator`.** The replication merger
  enters the coordinator directly and is therefore never counted.
- **Admission.** `NodeClose.awaitAppIdle(Duration)` delegates the wait to the guarded manager.
  New outer entries check `NodeClose.checkOpen()` while holding the section-count lock, closing
  the gap between admission and increment. **Do not call `StorageGraphCoordinator.drain()` for
  this.** `drain()` takes the
  write lock and would contend with a live merger. The coordinator's own admission stays open
  until the replication-side drain at stage 10.
- **Close stages** (`M/node/NodeLifecycle.java:647-764`), in order:
  1. (implicit) the `closing` flag is set by `closeNode`;
  2. **`app drain`**: `appSections.awaitIdle(graphDrainTimeout)`;
  3. maintenance scheduler;
  4. backup executor;
  5. storage executor;
  6. replication transport;
  7. position provider;
  8. retention;
  9. managers and collaborators;
  10. `graphCoordinator.drain()` (replication side, unchanged);
  11. embedded storage.
- **Timeout.** `PERUNCS_GRAPH_DRAIN_TIMEOUT_MILLIS` (default 5,000 ms; renamed `PERUNCS_…`
  in D1). On timeout, the app-drain stage throws `GraphDrainTimeoutException`; close stages 3–5
  still stop maintenance and executors, while stages 6–11 remain not ready, keeping transport,
  managers, graph drain, and Store open. `close()` reports the failure; a later call retries.
- **The same behaviour holds pre-A1 and post-A1.** The close never proceeds past a live
  application section.

**Files.** `M/node/store/GuardingStorageManager.java`, `M/node/store/NodeClose.java`
(`void awaitAppIdle(Duration)`), `M/node/NodeLifecycle.java`.

**Invariants.**
1. No application section runs when the transport stops.
2. The merger never counts.
3. A close from inside a section is still rejected (`NodeLifecycle.java:600-607`).

**Tests.**
- Close from thread B while thread A is inside `graphBoundary().write` at the
  `AFTER_PREPARE_BEFORE_LOCAL_WRITE` hook → A completes, B's close returns after A, and restart is
  `LIVE` without reseed.
- A section held past the timeout → `GraphDrainTimeoutException`, the transport is still open, and
  a second close after release succeeds.
- An exception thrown inside a section still decrements (the counter returns to 0).

**Implementation status (2026-09-30).** The lifecycle-level timeout test
`NodeLifecycleTest.applicationDrainTimeoutLeavesStoreOpenAndCloseCanRetry` covers the basic
retry. `AeronApplicationSectionDrainIT.closeWaitsForPreparedWriteAndRestartResumesWithoutReseed`
now uses the production `ClusterNode` close sequencer and covers the Aeron hook, transport
availability after timeout, retry, and restart. Both pass in the final integration profile.

### C3 — Backup API returns a future (P1)

**Decision.** D-07.

**API/format changes.**
- **`api` types:**
  - `enum BackupSlot { SCHEDULED, MANUAL }`;
  - `record BackupInfo(UUID id, Instant createdAt, long sequence, boolean manual)`, where
    `sequence` is -1 when unknown (D-16).
- **`ClusterNode.createBackup(BackupSlot)`** returns `CompletableFuture<BackupInfo>` and replaces
  `createScheduledBackup`/`createManualBackup`.
  - **Busy:** a future completed exceptionally with `BackupBusyException` (never thrown).
  - **Wrong role:** a synchronously thrown `WrongRoleException` (a programming error).
- **`StorageBackupTaskExecutor.runBackup(BackupSlot)`** returns the future, and the task body
  completes it. The reader stop-at-latest runs **at task start**.
- **Starter backup.** `NodeLifecycle.startBackupNode` calls
  `createBackup(SCHEDULED).get(backupCloseTimeout)` before `backend.deleteUserUploadedStorage()`.
  Failure or timeout fails startup, and the upload is kept.
- **`StorageBackupManager.createStorageBackup`** returns `BackupInfo`.

**Files.**
- `M/api/ClusterNode.java`
- new `M/api/BackupSlot.java`, `M/api/BackupInfo.java`
- `M/node/backup/{BackupNodeManager,BackupNodeControl,StorageBackupTaskExecutor,StorageBackupManager}.java`
- `M/node/NodeLifecycle.java`

**Tests.**
- Success yields `BackupInfo`.
- A concurrent second request → `BackupBusyException` future.
- A failing backend → exceptional future.
- A failing starter backup → startup fails and the upload is still present.

**Implementation status (2026-09-30).** Starter-backup timeout, failure, cancellation, and success
are covered at the `awaitStarterBackup` helper boundary. `NodeLifecycleStarterBackupTest` now starts
the real backup-node lifecycle with a restored upload and a backend that fails while creating the
starter backup; it asserts startup fails and the upload is retained. The test passes in the final
integration profile.

### C5 — `LazyHolder.get()` synchronized (P2)

`LazyConstant` serializes value creation, but its thread safety does not coordinate the separate
`isInitialized()` probe used by close. Keep `get()` and `isInitialized()` synchronized so close
waits for an in-flight factory and then sees the created resource before deciding whether its
stage can be skipped. The volatile-only variant fails this close race. **Decision:** retain the
small synchronized holder; A5's role-specific assembly classes are withdrawn because they would
duplicate the shared lazy-resource and close graph without removing the few role branches that
exist today.

### C6 — Reader-side registry mutators (P1)

**Decision.** In `BinaryPersistenceManagerAdapter` (`M/node/store/GuardingStorageManager.java:623-817`),
on read-only roles only:

| Method | Behaviour |
|--------|-----------|
| `ensureObjectId(Object)`, `ensureObjectId(U, requestor, handler)`, `ensureObjectIdGuaranteedRegister(...)` | Return `delegate.lookupObjectId(object)` if the object is registered; otherwise throw `ReaderWriteRejectedException`. |
| `mergeEntries`, `registerLocalRegistry`, `consolidate` | Throw `ReaderWriteRejectedException`. |
| `createRegisterer()`, `objectRegistry()` | Throw `ReaderWriteRejectedException` on readers. |
| Writer | All listed operations delegate unchanged, preserving Eclipse Store behavior. |

**Why internal loading is unaffected (verified):**
- Store loading never uses this adapter. `Lazy` binds to the `ObjectSwizzling` loader captured at
  load time (`$GITHUB_ROOT/eclipse-serializer/serializer/…/reference/Lazy.java:257-264`).
- GigaMap main sources do not call `persistenceManager()`, `ensureObjectId` or `createRegisterer`
  (grep of `$GITHUB_ROOT/eclipse-store/store/gigamap/*/src/main`).

**Tests.**
- Reader: each row's rejection; `ensureObjectId` of a loaded object returns its id.
- Regression: lazy traversal inside `graphBoundary().read` still works.

### C10 — Backup "running" state without wedging close (P2)

**Correction.** The rev-3 fix ("set the flag before `submit`, clear it in `finally`") is
**wrong**: a task cancelled while queued (`cancel(false)`) never runs its `finally`, so the flag
would stay set and the Store-close stage would never proceed. The existing comment at
`M/node/backup/StorageBackupTaskExecutor.java:154-157` exists for that reason. It also contradicts
its own code ("out of the body" versus the assignment inside the body, `:158`).

**Decision.** A three-state machine under the executor's `LockedExecutor` state lock:
`enum BackupPhase { IDLE, QUEUED, RUNNING }`, one field `phase`.
One write section keeps the phase, queued task and result future in each transition; a second atomic
state would not remove the lock needed by close and cancellation.

| Transition | Where |
|------------|-------|
| `IDLE → QUEUED` | `runBackup` reserves the slot under the state lock, then calls `submit` outside it. A runtime submission rejection restores `IDLE` and completes the future exceptionally; a fatal `Error` restores `IDLE` then rethrows. |
| `QUEUED → RUNNING` | First statement of the task body, under the state lock. If `phase != QUEUED` (the close path already reset it), return without running. |
| `RUNNING → IDLE` | Task `finally`, under the state lock. |
| `QUEUED → IDLE` | `close()`, under the state lock: `backupTask.cancel(false)`, then `if (phase == QUEUED) phase = IDLE`. |

`isRunningBackup()` returns `phase != IDLE` (the busy check) and `isBackupExecuting()` returns
`phase == RUNNING`. The Store-close gate in `NodeLifecycle` uses `isBackupExecuting()`.

**Tests.**
- Cancel while queued → the phase is `IDLE`, the close completes, and the body never runs.
- Close while running → close waits (bounded) and the phase ends `IDLE`.
- A submit rejection → the returned future fails and the phase is `IDLE`.

### A4 — Export the index registration facade (P1)

**Decision.**
- **Surface.** Exactly three methods are exported. The other public methods of
  `ClusterStoreIndexes` (`withRegistrationRead`×2, `validate*`, `validateStorageRoots`,
  `writerValidator`, `registerVector`) remain internal.
- **Module requirements.** The GigaMap, Lucene, JVector, and Lucene Core modules become
  `requires transitive`, because their types appear in exported signatures.

**API/format changes.** `peruncs.cluster.api.ClusterIndexes` (final, private constructor):

```java
public static <E> LuceneContext<E> embeddedLuceneContext(DocumentPopulator<E> populator); // → ClusterStoreIndexes.embeddedLuceneContext (:76)
public static <E> LuceneIndex<E>   registerLucene(GigaMap<E> map, DocumentPopulator<E> populator); // → registerLucene (:92)
public static <E> VectorIndex<E>   addVector(/* exact parameter list of ClusterStoreIndexes.addVector, :151 */); // → addVector (:151)
```

`src/main/java/module-info.java`: change `requires org.eclipse.store.gigamap`,
`requires org.eclipse.store.gigamap.lucene`, `requires org.eclipes.store.gigamap.jvector`,
`requires jvector` and `requires org.apache.lucene.core` to `requires transitive`. Keep the
upstream misspelling `org.eclipes`.

**Tests.**
- `T/probe/ModulePathRuntimeProbeTest.java`: no `--add-exports`; its named consumer compiles with
  only `requires peruncs.cluster` while using Lucene/JVector types from the facade.
- `T/probe/ModulePathProbeMain.java`: use only `peruncs.cluster.api.ClusterIndexes`.
- `AeronStoreIntegrationIT.aeronReplicatesEmbeddedLuceneAndVectorStateToAReader`: register both
  indexes through the facade on a writer, replicate, and query on a reader.

---

### A1 — Replication mark inside the Store commit (P1; decided)

#### A1.1 Decision

D-03, D-08, D-26. The writer's local commit and its replication position become one atomic Store
transaction. Recovery decides COMMIT or ABORT deterministically.

#### A1.2 S-0: prepare durability (answered from the Aeron source)

`awaitRecorded` (`M/storage/aeron/writer/AeronArchiveReplicationPublisher.java:279-345`) returns on
three paths:
- **(a)** the local `RecordingPos` counter;
- **(b)** `getStopPosition` when the recording is inactive;
- **(c)** a control-session `getRecordingPosition` when the counter is not visible (the external
  Archive).

For **(a)**, Aeron's `RecordingWriter.onBlock` writes the block and, when `fileSyncLevel > 0`,
calls `recordingFileChannel.force(fileSyncLevel > 1)` **before returning**
(`$GITHUB_ROOT/aeron-io/aeron/aeron-archive/src/main/java/io/aeron/archive/RecordingWriter.java:132-141`,
`forceWrites = ctx.fileSyncLevel() > 0`, `:88`). `RecordingSession.record()` publishes the counter
with `position.setRelease(...)` only **after** `image.blockPoll(recordingWriter, …)` returns
(`RecordingSession.java:236-241`). Segment rollover forces the archive directory as well
(`RecordingWriter.java:221-223`).

So for the embedded Archive with `fileSyncLevel ≥ 1`, `RecordingPos ≥ X` means bytes up to `X` are
on disk. Path **(b)** reads the stop position of a recording whose writes completed through the
same writer. Path **(c)** depends on a foreign Archive's configuration, so D-26 removes the
external mode and path (c).

`fileSyncLevel ≥ 1` in production is **already enforced** (`M/node/aeron/AeronSettings.java:326-333`);
only a test is added.

#### A1.3 Mark schema and identity

```java
final class ReplicationMark {           // internal; package M/storage/aeron/mark until D9
    UUID clusterId; UUID storeGeneration;
    long epoch; long recordingId; long fencingToken;
    long sequence;              // last sequence whose data is committed in this Store; -1 = none
    long prepareStartPosition;  // recording position where prepare(sequence) begins; recording start position when sequence = -1
}
```

- **Registration call site.** `NodeLifecycle.prepareEmbeddedStorage`
  (`M/node/NodeLifecycle.java:455-483`) is the Store bootstrap shared by every replicated role.
  After `setConfiguration`, it calls
  `foundation.getConnectionFoundation().getRootResolverProvider().registerRoot("peruncs.replication", mark)`
  (`$GITHUB_ROOT/eclipse-serializer/…/PersistenceRootResolverProvider.java:92`). WRITER, READER
  and BACKUP_READER register it; STANDALONE does not (no mark exists there).
- **Identity.** There is exactly one instance per node, never replaced.
  - **Writer.** It mutates the fields in place under coordinator admission and stores the instance
    in every commit. One object id, a new version per commit; Store GC reclaims old versions.
  - **Reader.** Store loads the persisted state into the registered instance at start. Each import
    carries a new version of the same object id, and the existing materializer
    (`M/storage/binary/StorageBinaryDataMaterializer.java`) updates **the same instance** in place,
    like any replicated object. The reader never stores it.
- **Thread safety.**
  - Writer: written and serialized by the thread holding coordinator admission; recovery runs
    before admission opens.
  - Reader: written by the materializer under the coordinator write side; others read a volatile
    `ReplicationPosition` snapshot published after each barrier.
- **Reserved identifier.** If the application registered `"peruncs.replication"`, startup throws
  `NodeException`.
- **Facade.**
  - `viewRoots()` omits the entry (both `iterateEntries` and identifier lookups).
  - `Database.getObject(markOid)` is readable (documented read-only).
  - `issueFullBackup` includes the mark (intended).
- **Existing Store without the root:**

  | Case | Result |
  |------|--------|
  | Writer with an empty recording | Initialize `sequence=-1`, `prepareStartPosition=recordingStartPosition`, `fencingToken=0`, persisted by the bootstrap commit (A2) |
  | Writer with a non-empty recording | `ReseedRequiredException` |
  | Reader or backup-reader | `ReseedRequiredException` |

#### A1.4 Writer protocol

This runs inside `persist` (under `writeExclusive`) and coordinator admission.

1. **Reserve.**
   - `S' = coordinator.nextSequence`; it is `mark.sequence + 1` unless an ABORT consumed sequences.
   - `start = exclusivePublication.position()`. With one publisher on one thread, the next frame
     begins there.
   - Set `mark.sequence = S'`, `mark.prepareStartPosition = start`, `mark.fencingToken = current`.
2. **Serialize.** One storer: `store(application objects…)`, `store(mark)`, `commit()`. The target
   receives one `Binary` containing both.
3. **Mark check.** In the target, the binary must contain an entity with the mark's object id,
   checked with the entity-header scanner shared with P1-1. Otherwise throw
   `WriteRejectedException("replicated commit without replication mark")`.
4. **Prepare.**
   - Offer the dictionary and data chunks, then `awaitRecorded(lastChunkEndPosition)` (path (a)).
   - **Timeout or failure:** offer `ABORT(S')`, then throw `WriteRejectedException`. The sequence
     is consumed, and the in-memory mark fields are overwritten by the next reservation.
5. **Local write** (`delegate.write`).
   - **Clean pre-write rejection:** ABORT + `WriteRejectedException`.
   - **Anything else:** latch. Recovery resolves it at the next start.
6. **COMMIT.** Offer `COMMIT(S')`. **Do not await** its recording.
   - **Offer failure after bounded retry** (`offerTimeoutNanos`): throw `ReplicationPendingException`
     (new, exported). The graph is **not** latched, and the writer enters `REPLICATION_SUSPENDED`:
     admission throws `WriteRejectedException("replication suspended: COMMIT(S') pending")`.
   - A retry task re-offers every `offerTimeoutNanos` and reopens admission on success. A restart
     also resolves it (A1.5).

**Which facade methods carry the mark:**

| Method | Mark |
|--------|------|
| `store`, `storeAll(Object...)`, `storeAll(Iterable)`, PM `store`/`storeAll` | One storer with the mark. |
| `Storer.commit()` (application storers) | The mark is added to that storer just before `commit()`. |
| `storeRoot()` | A storer stores `rootReference`, `root` (if non-null) and the mark. This mirrors upstream `storeAll(rootReference, root)`, `$GITHUB_ROOT/eclipse-store/…/EmbeddedStorageManager.java:238-251`; S-3 verifies it. |
| `setRoot` | None (in memory only). |
| PM `updateMetadata`, PM `updateCurrentObjectId`, `persistenceManager().target().write` | **REJECT** (`UnsupportedOperationException`) when `role ∈ {WRITER, READER, BACKUP_READER}`. STANDALONE keeps today's behaviour. The raw target is reachable only through `persistenceManager().target()`. |

Store-internal work (housekeeping, GC, dictionary export) does not write through the persistence
target. Step 3 catches any missed path.

#### A1.5 Writer recovery (before admission opens)

- **Inputs:** mark `M`, `p = prepareStartPosition`, epoch, recording, token, and recording
  `stopPosition`.
- **Window:** `W = p + 2 × (maxTransactionBytes + ceil(maxTransactionBytes / chunkSize) × 84) + 4 × 84`.
- **Replay** `[p, stopPosition)` once, locally. Evaluate the rules **top-down; the first match
  wins**.

| # | Condition | Action |
|---|-----------|--------|
| 1 | `stopPosition < p` | RESEED ("mark ahead of Archive") |
| 2 | `stopPosition > W` | RESEED ("tail exceeds recovery window") |
| 3 | Any frame whose cluster, epoch or recording differs from the mark, or whose token is > the mark's token | RESEED |
| 4 | `M ≥ 0` and frames of M are not a **complete prepare** | RESEED (unreachable in a healthy run: the prepare is awaited before the local write) |
| 5 | Terminal of M is `ABORT` | RESEED (contradiction) |
| 6 | Frames of M+1 present **and** terminal of M absent | RESEED (impossible ordering) |
| 7 | `COMMIT(M+1)` present | RESEED (a local write must precede COMMIT) |
| 8 | Any frame with a sequence > M+1 | RESEED |
| 9 | Otherwise | (a) if `M ≥ 0` and the terminal of M is absent → append `COMMIT(M)`; (b) if frames of M+1 exist without a terminal → append `ABORT(M+1)`; (c) `nextSequence` = M+2 if M+1 appeared, else M+1 |

**Complete prepare(n)** means:
- zero or more `TYPE_DICTIONARY` chunks with contiguous `chunkIndex` 0..k-1 and consistent
  `chunkCount`;
- then `STORE_BINARY` chunks 0..c-1, contiguous, with consistent `chunkCount`;
- every chunk passes the header checksum and payload CRC;
- the summed payload equals the declared `payloadLength`;
- all of it lies at positions ≤ `stopPosition`.

**Appending:** through the existing `extendRecording` path
(`M/storage/aeron/writer/AeronArchiveReplicationPublisher.java:223, 479-486`), awaited recorded.

**Availability:** readers wait at the tail until the writer restarts. Today the same crash requires
a reseed.

#### A1.6 Reader protocol

- **Start:** `initialSequence = mark.sequence`, `initialPosition = mark.prepareStartPosition`,
  fencing floor = `mark.fencingToken`.
- **Duplicates.** The assembler **skips without validation** every frame with
  `sequence ≤ initialSequence`, including the re-delivered prepare chunks and the `COMMIT` of
  `mark.sequence`. It resumes normal processing at the first frame with
  `sequence > initialSequence`. Existing witness validation
  (`M/storage/aeron/reader/TransactionAssembler.java:377-402`) applies only after a locally
  resolved sequence.
- **Aborts.** As today, including ABORT after a partial prepare
  (`AeronReplicationPublisher.java:343-345`).
- **Live COMMIT.** Applied immediately: COMMIT(n) is offered only after prepare(n) is recorded and
  n is committed locally, and recovery (A1.5 rule 9a) completes such a sequence, never aborts it.
- **Delete (reader):**
  - `CommitDurabilityGate` and its call (`TransactionAssembler.java:239-261, 297-335`);
  - the recorded-position refresher (`M/storage/aeron/reader/AeronArchiveReader.java:332-345, 550-593`);
  - `PERUNCS_AERON_LIVE_WITHHOLD_TIMEOUT_NANOS`;
  - `ReaderDeliveryListener` and the `.reader-inflight` uncertainty record
    (`M/node/aeron/AeronReaderTransport.java:278-300`, `readerUncertaintyPath`,
    `rejectUncertainReaderImport`);
  - `DurableCursorFile`, the `offset` file, and `NodeLifecycle.requireStoredCursorForExistingStore`.
- **Epoch.** Readers keep rejecting frames of another epoch (rule unchanged in A1). Only A2b
  relaxes it, and that edit belongs to A2b.

#### A1.7 Invariants

1. Local Store commit only after its prepare is recorded (path (a)).
2. COMMIT(n) only after the local commit of n.
3. The mark and data are in one Store commit.
4. Sequences are strictly increasing; ABORT consumes one.
5. `fileSyncLevel ≥ 1` in production (existing check, now tested).
6. One transaction in flight (D-23).
7. Embedded Archive only (D-26).

#### A1.8 Files

**Add:**
- `M/storage/aeron/mark/ReplicationMark.java`
- `M/errors/ReplicationPendingException.java`

**Delete:**

| File | Why |
|------|-----|
| `M/storage/aeron/checkpoint/AeronReplicationCheckpoint.java` | Every writer state and the `READER_CURSOR` record type are replaced by the mark |
| `M/storage/aeron/checkpoint/AeronReplicationCheckpointStore.java` | |
| `M/node/replication/DurableCursorFile.java` | |
| `M/node/replication/ReplicationCursorStore.java` | |
| `M/node/replication/CommitAppliedListener.java` | |
| `M/storage/aeron/reader/ReaderDeliveryListener.java` | |

**Edit** (every current importer of the deleted types, verified by grep):

| File | Edit |
|------|------|
| `M/node/aeron/AeronWriterTransport.java` | Checkpoint reconciliation (`:628-730`) → A1.5 recovery |
| `M/node/aeron/AeronReaderTransport.java` | Resume from the mark; remove the uncertainty record |
| `M/node/aeron/AeronTransport.java` | Checkpoint-path wiring and metadata-storage probe for checkpoints |
| `M/node/NodeAssembly.java` | Remove the `DurableCursorFile` holder, the applied listener and the `offset` path |
| `M/node/NodeLifecycle.java` | Mark registration; remove the cursor gates |
| `M/node/replication/ClusterReplicationTransport.java` | Remove `CommitAppliedListener` / cursor parameters |
| `M/node/backup/BackupRestorePolicy.java` | Keep: identity/compatibility selection and "writer never restores over existing local state". Delete: `cursorManager`, `closeCursorManager`, `deleteOffsetFile`, and the local-cursor comparison (`:188-224`); a restore installs the Store, whose mark is its cursor |
| `M/node/backup/FilesystemVolumeBackupBackend.java` | Remove the `manifest` read/write (`ReplicationCursorStore.encode/decode`); `getCursorForBackup` reads the mark from the extracted Store's metadata instead (or is deleted if identity comes from the backup identity sidecar, S-1) |
| `M/node/backup/BackupArchive.java` | Remove `MANIFEST_ENTRY` requirements |
| `M/node/backup/BackupMetadata.java` | `requireConsistentWithCursor` deleted |
| `M/storage/aeron/position/AeronPositionCodec.java` | Keep the shared metadata codec for live positions and watermarks; old checkpoint package removed |
| `M/storage/aeron/position/AeronReaderWatermark.java` | Keep the watermark format outside the deleted checkpoint package |
| `M/storage/aeron/reader/{AeronArchiveReader,TransactionAssembler}.java` | Per A1.6 |
| `M/storage/aeron/writer/{AeronArchiveReplicationPublisher,AeronReplicationWriteCoordinator,AeronReplicationPublisher,AeronStorageBinaryReplicationTarget}.java` | Per A1.4, D-26; remove `CheckpointWriter` and the PREPARING/COMMITTED/COMMITTING_UNCERTAIN/REJECTED notifications |
| `M/storage/package-info.java` | Update the reference to `AeronReplicationCheckpoint` |
| `M/node/aeron/AeronSettings.java` | Remove `EXTERNAL_ARCHIVE`, `CHECKPOINT_PATH`, `LIVE_WITHHOLD_TIMEOUT_NANOS` |

**A3 status:** `ReplicationCursor` and `AeronReplicationCursor` are deleted. The typed
`ReplicationPosition` is the in-memory boundary; `AeronPositionCodec` remains for the watermark wire
format. Runtime decisions use the typed Store mark instead of transport-id strings. D1 supplies
typed `NodeConfig` role and transport values throughout runtime assembly.

**Docs:** README ("Store binary transport", crash-matrix oracle, seeding, remove checkpoint-path
settings) and the `module-info` Store-mark / seeding sections.

#### A1.9 Spike (step 2)

- **S-1 (named root on the reader path).**
  1. Writer Store W registers the root, stores the mark and one object.
  2. Copy W to reader Store R.
  3. R registers an instance and starts.
  4. W commits 100 transactions, each imported and materialized in R.
  5. Assert that R's fields equal W's and that the instance identity is unchanged.
  - **Fail** → fallback: an internal default root `ClusterRoot{Lazy<T> user; ReplicationMark mark}`,
    hidden by the facade (`root()` returns `user`).
- **S-2 (atomicity).** A forked child writes 4 MiB of objects plus the mark in one
  `Storer.commit()`, and is killed (`kill -9`) at a uniform 0–50 ms delay inside `commit()`, 200
  trials. After restart, assert (mark = n and all of n present) or (mark = n-1 and none of n).
- **S-3 (`storeRoot`).** The custom `storeRoot` produces the same persisted roots as upstream, on a
  fresh and on an existing Store.
- **S-4 (dictionary).** The mark type is merged into the reader dictionary before its first import.
- **S-5 (recovery).** For each A1.5 rule, synthesize the tail with the existing crash hooks and
  assert the action, with replay ≤ `W − p` bytes.

**S-1 result (2026-09-28): PASS.** `AeronStoreIntegrationIT.namedReplicationRootKeepsItsIdentityAcrossOneHundredImports`
registers the same named-root identifier on the writer and reader, persists the root with user
data, copies the seed Store, then imports 100 writer transactions. The reader's original root
instance resolves by its persisted object id after every import and exposes each new sequence and
value. Registering the named root on every new Store foundation is required before `start()`.

**S-3 result (2026-09-28): PASS.**
`AeronStoreIntegrationIT.storerRootSnapshotMatchesStoreRootOnFreshAndExistingStores` writes the
default root and a named mark in one `storeAll` call, then compares a fresh Store and a root
replacement on an existing Store against upstream `storeRoot()`. Existing-object field mutation is
intentionally excluded: Store documents that `storeRoot()` only stores a new root or a previously
unknown graph, and does not persist arbitrary changes to already-known objects.

**S-2 result (2026-09-30): PASS.**
`ReplicationMarkCrashMatrixIT.namedMarkAndFourMegabytesOfStoreDataRecoverTogetherAfterProcessKill`
seeds production `ReplicationMark`, then kills the child JVM 0–50 ms after the 4 MiB commit enters
the Store target. Each of 200 trials restarts Store and checks that the mark and all payload
entities are either present together or absent together. The crashmatrix profile ran and passed
all 200 trials.

#### A1.10 Tests

- **Crash matrix:** new oracle `CONTINUE`, except rules 1–8 → `RESEED_REQUIRED`; checkpoint cells
  deleted; one cell per A1.5 rule.
- **Reader resume:** in-flight S+1 later committed / later aborted → correct Store; duplicates
  ≤ the mark are skipped.
- **`ReplicationPendingException`:** block COMMIT offers → exception, graph valid, suspended;
  release → resumes; restart instead → COMMIT appended.
- **`fileSyncLevel=0` in production** → startup fails (existing check, new test).
- **Benchmark harness** (steps 0 and 4):
  - **Setup:** Linux, local NVMe, `fileSyncLevel=1`, 1 writer + 3 readers on loopback, payloads of
    1 KiB and 64 KiB, 4 writer threads, 60 s warm-up, 5 × 60 s measured.
  - **Metrics:** writer commit p50/p99 and commits/s; reader apply p99 (import start → mark
    visible); live end-to-end p99 (`store()` return → reader mark ≥ n); and, for C4,
    `graphBoundary().read` p99 under write load.
  - **Tooling:** a JFR collector (existing `SoakJfrReport` pattern) writes
    `bench/results/<commit>.json`; `bench/compare` enforces D-25; profile `-Pbench`, not in the
    default gate.

---

## 4. Specifications — steps 5–11

### A2 — Delete the NFS fencing lease (P1)

**Decision.** D-04.

**Status (2026-09-30): DONE.** The shared NFS lease, writer lease gate, lease settings, and their lease-only tests are deleted. `writer.lock` is held before Store/Aeron startup and released after the Store close stage. Existing Store marks are advanced by one replicated bootstrap commit per writer startup, while fresh marks start at token 1. The reader throws `CorruptReplicationDataException` for a lower token. Lock/token tests, repeated-start coverage, and final integration/crashmatrix profiles pass.

**Changes.**
- **`writer.lock`.** `<storagePath>/writer.lock`, opened (`CREATE, WRITE`, owner-only) and locked
  with `FileChannel.tryLock()` **before** Store or Aeron start.
  - `null` or `OverlappingFileLockException` → `NodeException("another writer process holds <path>")`,
    and nothing has started.
  - It is released after the Store close stage.
- **Bootstrap commit.** After A1 recovery and before admission:
  - one replicated transaction (sequence `nextSequence`) storing only the mark, with
    `fencingToken = previous + 1` (1 on a fresh cluster);
  - the **epoch is unchanged**. The epoch changes only with a new recording (a new cluster, or
    A2b);
  - readers raise their token floor when they apply it.
- **Admission after A2:** only publisher state (`isFailed`, `REPLICATION_SUSPENDED`), the Archive
  capacity predicate and the maintenance flag. The coordinator, `EnvelopeFramer`, and
  `AeronOfferRetryer` no longer carry a lease gate or gated-offer path.
- **`WriterFencedException`:** deleted from `errors`. A reader seeing a lower token throws
  `CorruptReplicationDataException` (reader FAILED).
- **Delete:**
  - `M/node/aeron/WriterFencingLease.java`, `M/storage/aeron/writer/WriterLeaseGate.java`;
  - lease-directory validation (`M/node/aeron/AeronTransport.java:103-140`);
  - settings `…_LEASE_PATH`, `…_LEASE_STALENESS_MILLIS`, `…_LEASE_LOCK_TIMEOUT_MILLIS`,
    `…_SHARED_LEASE_FILESYSTEM`;
  - the NFSv4 check;
  - the README "Network boundary" NFS paragraphs.
- **Documented residual risk.** `writer.lock` protects one Store path on one host. Two hosts
  writing to the same cluster from different Store paths is an operator error that the library
  cannot detect beyond the reader token check.

**Tests.**
- A second writer on the same storage fails at startup, and the first is unaffected.
- The token equals N after N starts (asserted by the three-phase restart smoke test).
- A reader rejects a synthesized lower-token frame as `CorruptReplicationDataException`.

### A2b — Manual promotion without reseed (optional, P3; needs A8)

**Admin API:** `ClusterNode.promoteToWriter(long newEpoch)`, allowed only for READER when stopped
at the latest boundary.

**Procedure:**
1. Stop the old writer.
2. Wait until the chosen reader's mark equals the last COMMIT of the old recording.
3. Restart it as WRITER with `epoch = old + 1`: it creates a new recording (alias per A8), starts
   at `mark.sequence + 1`, and runs the bootstrap commit.
4. Redirecting application writes is a deployment concern.
5. Other readers follow when `reader.mark.epoch == old` and
   `reader.mark.sequence == newWriter.startSequence − 1`; otherwise reseed.

**This item owns the edit** that relaxes the reader's epoch check for exactly that condition.

### P1-1 — Per-commit index validation pre-filter (P1)

**Problem (cited).**
- `ClusterIndexValidation.countScanWork` throws beyond `DEFAULT_MAX_VALIDATED_OBJECTS = 65_536`.
- The writer validator hard-codes that default (`M/storage/index/ClusterStoreIndexes.java:297-307`),
  and the error names the merger's setting, which the writer does not read.
- Opaque stateful `java.*` holders also throw (`M/storage/index/ClusterIndexValidation.java:446-456`).

**Changes.**
- **Shared scanner (built per F1).** Extract the bounds-checked entity-header scanner from
  `M/storage/index/ClusterIndexMaintenance.java:34-70` into `M/storage/index/EntityHeaders.java`,
  usable for a writer-held `Binary` (`binary.iterateEntityData(reader)` → scan each buffer) and for
  imported buffers. `EntityHeaders` and its visitors stay package-private. The materializer uses
  the narrow `ClusterStoreIndexes.validateEntityFraming` bridge across packages. It uses
  Serializer's entity header layout (length, typeId, objectId), as today.
- **Pre-filter.** `ClusterIndexValidation.commitTouchesIndexes(Binary, PersistenceTypeHandlerManager)`:
  for each entity type id → handler → `type()` → the existing `INDEX_RELEVANT` `ClassValue`.
- **Gate.** `AeronStorageBinaryReplicationTarget.prepareWrite` runs the full validation only when
  the pre-filter returns true. A rejection is a `WriteRejectedException` (C1).
- **Setting.** `PERUNCS_INDEX_VALIDATION_MAX_OBJECTS` (default 65,536) is an enum-backed
  `NodeConfig` setting shared by the writer and merger. The error message names the same key.
- **Fallback if the proof test fails:** keep the per-commit full scan, with the configurable bound
  and the corrected message.

**Tests.**
- **Proof:** register an external-directory Lucene index directly on an already-stored GigaMap,
  then `storage.store(map)` (the commit carrying the new index group) → `WriteRejectedException`
  caused by `IllegalArgumentException(EXTERNAL_LUCENE_MESSAGE)`; the graph stays valid; the
  application must remove the registration (documented).
- 10k plain entities → the full scan is not invoked (spy).
- Over the bound → a clean rejection naming the key.

### F1 — Bounds-checked parsing of replicated bytes (P1; step 6, with P1-1)

**Problem.** Replicated Store binaries can be malformed by corruption or software bugs. The
cluster requirement is no node authentication and no transport encryption; framing validation
handles malformed bytes and does not authenticate or authorize publishers. Binaries were walked
with raw native address arithmetic through Serializer helpers:
- the entity-header walk in `M/storage/index/ClusterIndexMaintenance.java:34-70`, which fails on
  a "truncated imported entity header";
- the materializer's `BinaryEntityRawDataIterator.iterateEntityRawData(address, address + limit, …)`
  over `getDirectByteBufferAddress(buffer)` in `M/storage/binary/StorageBinaryDataMaterializer.java`.

A corrupt entity length can drive these reads out of bounds, and such a native read is unchecked:
it yields a crash or garbage, not an exception.

**Decision.** D-29. PerunCS-owned parsing uses bounds-checked absolute reads after checking buffer
bounds and framing. Native-order views use `ByteBuffer.getLong`; non-native-order views use a
VarHandle configured for Serializer's native byte order. Reads do not change caller state or copy
payloads. Serializer's upstream materializer
iterator remains the deliberate D-29 exception: it consumes a direct-buffer address, but only
after this framing validation has checked every item length.
- **Serializer's own `BinaryEntityRawDataIterator`,** used inside the materializer, is upstream
  code and stays as is. PerunCS validates the entity framing with F1 *before* handing the buffer to
  it, so the iterator never sees a length that runs past the buffer.

**API/format changes.**
- **New scanner.** `M/storage/index/EntityHeaders.java` is package-private. The materializer calls
  `ClusterStoreIndexes.validateEntityFraming` as the narrow cross-package bridge. Scanner visitors
  and implementation details remain package-private:
  ```java
  // Serializer writes entity headers in native order through XMemory.
  static void forEach(ByteBuffer directBuffer, EntityVisitor visitor);   // absolute native-order reads, with validated bounds
  static void forEach(Binary binary, EntityVisitor visitor);             // iterateEntityData(reader) → scan each buffer
  static void forEachTypeId(Binary binary, TypeIdVisitor visitor);       // writer pre-filter, type-id read
  // ClusterStoreIndexes.validateEntityFraming(ByteBuffer) is the cross-package bridge.
  @FunctionalInterface interface EntityVisitor { void entity(long typeId, long objectId); }
  @FunctionalInterface interface TypeIdVisitor { void typeId(long typeId); }
  ```
- **Format:** the scanner uses Serializer's native-order entity header layout, including 8-byte
  length, type-id and object-id fields. The old PerunCS byte walk has been removed; the upstream
  `BinaryEntityRawDataIterator` equivalence check passes for 10,000 real Serializer-produced
  entities.
- **Users:**
  - P1-1 `commitTouchesIndexes` (writer `Binary`);
  - A1.4 step 3 mark check (writer `Binary`);
  - reader index maintenance (replaces the raw walk in `ClusterIndexMaintenance`);
  - `StorageBinaryDataMaterializer`, which calls `validateFraming` on every buffer before the
    Serializer iterator.
- **Failure mapping:** a framing violation →
  `CorruptReplicationDataException("replicated entity framing is invalid at offset …")`, which fails
  the reader closed (existing semantics for corrupt data). Visitor exceptions propagate unchanged.

**Files.** New `M/storage/index/EntityHeaders.java`, `M/storage/index/ClusterIndexMaintenance.java`,
`M/storage/binary/StorageBinaryDataMaterializer.java`, and the P1-1/A1 call sites.

**Invariants.**
1. PerunCS-owned parsing uses no raw-address reads. The Serializer iterator used as the materializer
   still reads through a direct-buffer address after framing validation; this is the D-29 exception.
2. Buffer reads are native-order and bounds-checked before each header access.
3. There is no heap copy of payload bytes.

**Tests.**
- **Fuzz:** truncated buffers, a length past the end, a zero or negative length, and a header
  split across the end, each → `CorruptReplicationDataException`, never a JVM crash. 10k seeded
  random corrupt length fields are rejected by both framing validation and header scanning.
- **Equivalence:** for 10,000 entities in real Serializer output,
  `EntityHeaders.forEach` yields exactly the entities reported by
  `BinaryEntityRawDataIterator`.
- **Micro-benchmark** (JMH, `-Pbench`): compare the production type-only writer pre-filter on
  64 KiB and 1 MiB Serializer binaries against the upstream raw type-id walk. The 5% gate remains
  open and is not claimed complete.

**Out of scope.** Aeron envelope decode (`AeronReplicationEnvelope`), which stays on Agrona
`DirectBuffer` (D-29): Aeron hands us its term buffer as a `DirectBuffer`, and its reads are already
bounds-checked by Agrona when `agrona.disable.bounds.checks` is not set. Keep that property unset.

### P1-7 + N1 — One apply thread, reader-owned buffer pool (P1)

**Decision.** D-24, D-27.

**Changes.**
- **`Barrier`.** `record Barrier(ByteBuffer[] buffers, int bufferCount, long bytes, long firstSequence,
  long lastSequence, long lastPosition, String dictionary /* "" = none */, boolean hasData,
  long[] generations /* checked mode only, else empty */)`.
- **Producer side (Aeron poll thread, the sole producer).**
  - The fragment handler keeps returning `Action.BREAK` when the assembler's barrier is full
    (existing).
  - After each `controlledPoll`, the poll thread `offer`s the completed barrier to an Agrona
    `OneToOneConcurrentArrayQueue<Barrier>` (capacity 64). SPSC is correct because the poll thread
    is the only producer and the apply thread the only consumer.
  - There is an additional byte bound: `queuedBytes` (`AtomicLong`) ≤ merger `cachedBytesLimit`
    (existing `PERUNCS_DATA_MERGER_LIMIT`, default 64 MiB).
- **Queue full.** The poll thread keeps the barrier (it still owns the buffers) and **does not call
  `controlledPoll`** until the offer succeeds (duty-cycle idle). The subscription then stops
  consuming: Aeron flow control back-pressures the live image, and replay pauses. No drop, no
  failure, no blocking inside a fragment callback.
- **Consumer (apply thread `peruncs-apply`), per barrier:**
  1. dictionary merge;
  2. import (coordinator write side);
  3. materialize;
  4. retire Lucene views / JVector graphs (J1-a);
  5. release the write side;
  6. vector warm-up under a read section (after J1-a);
  7. publish `ReplicationPosition`;
  8. release buffers to the pool;
  9. `queuedBytes -= bytes`.

  It stamps `volatile Phase phase` (`IDLE, IMPORT, MATERIALIZE, INDEX_RETIRE, WARMUP`) and
  `volatile long phaseStartedNanos`.
- **Budget.** The poll thread checks, once per duty cycle, that
  `phase ∈ {IMPORT, MATERIALIZE, INDEX_RETIRE}` has not exceeded `materializationBudgetMs`, and
  `WARMUP` not the refresh budget. On expiry it latches the reader failure
  `ReplicationUnavailableException("apply phase <phase> exceeded <n> ms")`, and the node reports
  FAILED. The apply thread is not interrupted (the same as today's watchdog).
- **Synchronous "wait until applied"** (used by backup stop-at-latest):
  `ReaderComponent.stopAtLatest()` completes when the poll thread has reached the live tail and
  stopped polling, **and** the queue is empty, **and** `phase == IDLE`. `stopResult()` then
  reports `RESOLVED_BOUNDARY` with the last published `ReplicationPosition`.
- **Close:** stop polling → the apply thread drains the queue within `graphDrainTimeout` → the
  remaining barriers' buffers are released → join.
- **Pool (N1).**
  - The reader component owns one `NativeBufferPool` and passes it by constructor to the assembler
    and the apply thread.
  - Make it mandatory: delete every `pool == null` branch in `M/storage/binary/StorageBinaryDataImporter.java`.
  - The retention cap becomes a temporary key `PERUNCS_POOL_MAX_RETAINED_BYTES` (default
    32 MiB; D1 later).
  - Keep the `// ponytail:` rationale: the 32 MiB ceiling is deliberate and is raised only after
    profiling shows larger buffers recur.
  - Pool storage is arena-backed if F2 passes (below); otherwise it keeps `XMemory`
    allocate/free.
- **Checked mode** (system property `peruncs.pool.checked=true`, set by surefire/failsafe; off in
  production):
  - an `IdentityHashMap<ByteBuffer, Long>` of generations guarded by the pool's lock;
  - `release` increments the generation and poisons (capacity ≤ 4 KiB → all bytes `0xDE`; else
    the first and last 64 bytes);
  - the apply thread compares the barrier's recorded generations at import entry and at
  materialize entry;
  - a mismatch → `CorruptReplicationDataException("native buffer reused while owned")` → the
    reader latches. Implemented in the checked-only pool and apply-queue path; production with the
    property off allocates no generation arrays or identity map and performs no generation checks.
- **Ownership shape:** Keep package-private `ApplyQueue` and `ApplyWorker` as separate owners of
  queued-buffer accounting and Store materialization. Folding them into the public merger would make
  that type larger and remove the focused budget-test seam without reducing runtime work.
  `MergerLifecycle` stays package-private so tests can exercise a wedged worker without building a
  full Store merger; it adds no production thread or allocation.
- **Timing:** There is no standalone coalescing timer; `ApplyQueue` wakes the worker with its existing
  condition. Keep the one watchdog thread because a Store callback that ignores interruption cannot
  report its own timeout; the watchdog fails the reader while retaining buffers until the callback
  releases ownership.
- **Receiver contract:** Keep the ownership, non-blocking admission, and `awaitApplied` callbacks on
  `StorageBinaryDataReceiver`. The assembler uses them to stop polling before a bounded pool wait,
  transfer buffers exactly once, and acknowledge only after materialization. Removing the adapter
  would either copy every completed binary or block inside Aeron's delivery callback.

**Tests.**
- **Stress:** checked mode retains at most one buffer globally; `StorageBinaryPoolOwnershipIT`
  applies 10k real Store transactions with random 1 B–1 MiB payloads. Writer CRC32C is compared
  with each reader materialization. The fixture passes in `-Pintegration` with zero mismatches or
  corruption.
- **Budget:** a blocking Store stub → FAILED after the budget, and the poll loop stays alive.
- **Back-pressure:** a slow apply stub → the queue fills, polling stops, no loss, then catch-up.
- **Backup:** stop-at-latest waits for the queue to drain.

### F2 — Arena-backed native memory for PerunCS-owned buffers (P2; spike inside step 7)

**Scope.** Only native memory PerunCS allocates and frees itself:
- the assembler's transaction buffers (`M/storage/aeron/reader/TransactionAssembler.java:833-857`);
- `NativeBufferPool`;
- the merger/importer copies (`M/storage/binary/StorageBinaryDataImporter.java`);
- the writer staging buffer (`M/storage/aeron/writer/AeronReplicationPublisher.java:153`,
  `framingStorage`).

Today pooled buffers and writer staging use `NativeMemory`, backed by Serializer's
`XMemory.allocateDirectNative` / `XMemory.deallocateDirectByteBuffer`; empty sentinels and the
backup-copy fallback use `ByteBuffer.allocateDirect`.

**Decision.** D-29. Spike first; adopt only if both gates pass.

**Status (2026-09-30).** Spike gate 1 passes: `StorageBinaryImportIntegrationTest` imports actual
Serializer transactions from shared-Arena-backed buffers, closes Store before the Arena, and then
reopens the imported image. `NativeMemoryTrackingTest` passes and verifies NMT's `Other` committed
memory falls after the native pool closes. PerunCS-owned buffers now use shared FFM arenas. Each
buffer allocation has one arena, retained buffers are reused, and pool eviction or close releases
the corresponding arena. This keeps native memory bounded under churn while avoiding an arena per
transaction. The prior `XMemory` deallocator path did not free memory with the configured Java 27
Serializer accessor. Gate 2 (reader apply p99 and JMH allocate/release within D-25) remains deferred
by the owner; performance acceptance remains open.

**Design.**
- **Arena.** `Arena.ofShared()` owns each allocated buffer. Shared arenas permit buffers to cross
  from the poll thread to the apply thread (P1-7), and closing one releases an evicted allocation
  deterministically.
- **Pool on top of arenas.** Keep the D-27 power-of-two free lists. A pool allocation stores the
  buffer with its arena; reuse keeps the arena open, while eviction or pool close closes that
  buffer's arena. The pool does not create an arena per transaction.
- **Writer and receiver allocations.** The `NativeMemory` facade keeps the arena associated with
  buffers returned through the ByteBuffer API and closes it on explicit release. The writer staging
  buffer uses the same ownership boundary.
- **Temporal safety is not gained inside the pool** (memory is recycled), so the N1 checked mode
  (generation plus poisoning) stays. Accessing a buffer view after its arena closed does throw,
  which covers close-time bugs.
- **Replace** `XMemory.allocateDirectNative`/`deallocateDirectByteBuffer` for PerunCS-owned
  buffers. PerunCS must **never** call `XMemory.deallocateDirectByteBuffer` on an arena-backed
  buffer. The unexported `storage.binary` package owns this rule through `NativeMemory`.
- **Writer staging buffer.** The writer's single staging buffer becomes a segment from the
  publisher's arena. If P1-6 (gathering offer) lands first, the staging buffer disappears and this
  item shrinks.

**Spike gates (both required).**
1. **Store never frees our buffers.** Verify in `$GITHUB_ROOT/eclipse-store` and
   `$GITHUB_ROOT/eclipse-serializer` that the `importData(XGettingEnum<ByteBuffer>)` path and the
   `ChunksWrapper`/`Binary` paths used by PerunCS never call
   `XMemory.deallocateDirectByteBuffer` (or a buffer cleaner) on the caller's buffers. Add a test:
   import arena-backed buffers, close the Store, then close the arena, with no crash and no
   double-free.
2. **No throughput loss.** The benchmark (§3.A1 harness, reader apply p99, plus a JMH
   allocate/release loop) is within D-25 of the `XMemory` variant.

**Performance gate.** If the deferred D-25 comparison misses its threshold, optimize the allocator
or pool behind `NativeMemory`; reverting to the current Serializer deallocator is not an option
because it leaves native memory allocated after explicit release.

**Files.** New `M/storage/binary/NativeMemory.java`, `NativeBufferPool.java`,
`StorageBinaryDataImporter.java`, `M/storage/aeron/reader/TransactionAssembler.java`,
`M/storage/aeron/writer/AeronReplicationPublisher.java`.

**Invariants.**
1. All PerunCS native allocations go through `NativeMemory`.
2. No individual free of arena memory.
3. The arena is closed exactly once, at component close, after the apply thread has joined.

**Tests.**
- Spike gate 1.
- Leak test: native memory tracking (`-XX:NativeMemoryTracking=summary` in a forked test) returns
  to baseline after node close.
- The N1 stress test passes on the arena-backed pool.

**Out of scope.**
- Aeron's own term buffers (owned by Aeron).
- Store-internal buffers.
- The small metadata codecs (cursor, checkpoint, lease, watermark): not hot, and mostly deleted by
  A1/A2.

### C4 — Writer graph write-lock hold (P1; step 8)

**Decision.** Keep `writeExclusive` for writer `persist`. After A1 + A2 the hold is: serialize +
prepare + prepare recorded-wait + local fsync + COMMIT offer. Measure `graphBoundary().read` p99
under write load (§3.A1 harness).

Only if it exceeds 2 × the idle read p99: release the lock after `delegate.write` and before the
COMMIT offer (the offer needs only coordinator admission).

### J1-a — Adopt `VectorIndex.invalidateGraph()` (eclipse-store PR #832) (P1; with P1-4)

**Upstream.** [PR #832](https://github.com/eclipse-store/store/pull/832) remains open as of 2026-09-29 and proposes `VectorIndex.invalidateGraph()`:
- it takes `builderLock.writeLock()` and no parent-map monitor;
- it discards `deferredBuilderOps`;
- it closes and clears the builder and graph;
- it clears `graphRebuilt` last, so the next search/mutation/`optimize` rebuilds lazily;
- **it throws `IllegalStateException` in incremental on-disk mode.**

It confirms that Lucene needs nothing new, because `LuceneIndex.close()` is public and reusable.
PerunCS already uses it (`M/storage/index/ClusterIndexMaintenance.java:429-455`).

**Changes.**
1. **Retire.** Replace the field surgery in `ClusterIndexMaintenance` (`:345-375`) with
   `index.invalidateGraph()`, called inside the materialization write section.
2. **Delete** `StoreIndexReflection.VectorGraphFields` / `vectorGraphFields` and the
   `write`/`writeBoolean`/`readBoolean` helpers. After this, no write into upstream private state
   remains.
3. **P1-4.** Keep `ensureVectorSearchGraphs` (`ClusterIndexMaintenance.java`: top-1 probe search per
   dirty index) inside the coordinator write section. A focused race showed that JVector's first-use
   initialization is not safe against a concurrent first search, so warm-up must finish before
   application reads re-enter.
   - Delete the reflective guard check (it is now upstream's tested contract,
     `VectorIndexInvalidateGraphTest`).
4. **On-disk mode.** `ClusterIndexValidation` already rejects on-disk/external vector indexes, so
   `invalidateGraph()`'s `IllegalStateException` is unreachable. If it is ever thrown, latch the
   reader (`CorruptReplicationDataException`).

**Current status (2026-09-30).** The interim invalidation is centralized, and changed graphs warm
inside the coordinator write section before application reads re-enter. Warm-up no longer reads the
private rebuild flag. All local profiles pass. The upstream API replacement and the
1k-versus-100k write-section duration acceptance measurement remain open.

**Interim (before #832 merges).** Route current field writes through the existing centralized
upstream reflection boundary, `StoreIndexReflection.invalidateVectorGraph(VectorIndex<?>)`, so
adopting the API is a one-call-site replacement without another helper type.

**Tests.**
- `T/node/aeron/ReaderLiveIndexFreshnessTest` and the soak JVector checks still pass.
- The write-section duration does not scale with vector count (1k vs 100k, bounded ratio).
- An on-disk vector index is rejected at validation.

### J1-b — Read-only index discovery without PerunCS reflection (P2; step 12)

| Read | Supported path used by PerunCS |
|------|-------------------------------|
| Registered `GigaIndices` groups | `PersistenceTypeHandler.iterateInstanceReferences` on the registered GigaIndices handler |
| Lucene context | `PersistenceTypeHandler.iterateInstanceReferences` on the registered Lucene handler |
| Reachable references and declared member types | Serializer handlers plus `PersistenceTypeDefinition.instanceMembers()`; unknown/custom layouts remain fail-closed |
| (upstream, not a PerunCS read) Serializer `XMemory` built on `jdk.internal.misc.Unsafe` | `--add-exports java.base/jdk.internal.misc` remains required; moving `XMemory` off-heap access to FFM would remove the flag for Eclipse Store users (D-29). |

**Status: IMPLEMENTED (2026-09-30).** Group enumeration, Lucene-context lookup, and root traversal use
Serializer's supported reference and runtime type-description APIs. PerunCS production
`java.lang.reflect` use is limited to the JVector invalidation workaround for open PR #832; no other
PerunCS production reflection remains. Final index/integration profiles pass. The JVector workaround
and scan benchmark remain open under J1-a; the serializer's own `XMemory`/Unsafe dependency remains
upstream work tracked above.

---

## 5. Specifications — steps 9–10

### A9 — Eclipse Store API compatibility (P1)

**Decision.** D-02, D-10, D-11, D-14. An Eclipse Store application changes only how it obtains the
manager.

**Porting map** (README):

| Plain Eclipse Store | PerunCS Cluster |
|---------------------|-----------------|
| `EmbeddedStorage.start(root, path)` | `ClusterStorage.start(() -> root)` (config from the environment) or `ClusterStorage.start(() -> root, NodeConfig)` |
| `EmbeddedStorage.Foundation(path)…start()` | `ClusterStorage.Foundation()…start()` |
| `EmbeddedStorageFoundation` tuning | `setEmbeddedStorageFoundation(f)`: preserved, except the live file provider (from `NodeConfig.storage().root()`) |
| `root()`, `setRoot`, `storeRoot`, `store`, `createStorer`, `shutdown`, try-with-resources | Unchanged API, with the deviations listed below |

**API** (`peruncs.cluster.api`):

```java
public final class ClusterStorage {                       // mirrors EmbeddedStorage; private constructor (rule 18)
    public static <T> ClusterStorageManager<T> start(Supplier<? extends T> rootSupplier);
    public static <T> ClusterStorageManager<T> start(Supplier<? extends T> rootSupplier, NodeConfig config);
    public static <T> ClusterStorageFoundation<T> Foundation();
}
public final class ClusterStorageFoundation<T> {          // mirrors EmbeddedStorageFoundation naming; immutable result, fluent setters
    public ClusterStorageFoundation<T> setRootSupplier(Supplier<? extends T> supplier);          // required
    public ClusterStorageFoundation<T> setEmbeddedStorageFoundation(EmbeddedStorageFoundation<?> f);
    public ClusterStorageFoundation<T> setNodeConfig(NodeConfig config);                         // default NodeConfig.fromEnvironment(), resolved at start
    public ClusterStorageManager<T> start();       // starts Store, Aeron runtime, replication, maintenance; manager.shutdown() closes all
    public ClusterNode<T> startNode();             // same start; returns the control handle (status, backups); node.storageManager()
}
```

**Root contract.** The supplier may return `T`. `NodeLifecycle.initializeRoot` stores it wrapped in
`Lazy.Reference` (`M/node/NodeLifecycle.java:510`), and `root()` returns `Lazy<T>`. A supplier
returning a `Lazy` is used as is.

**Coordination (D-11).** `GraphBoundary` is the only coordination API. The merger accepts the
standard-library `Consumer<Runnable>` update seam, and production wires it directly to
`StorageGraphCoordinator.write`; no custom graph-update SPI remains.

**Deviations from Eclipse Store** (a method-level list in the `ClusterStorageManager` Javadoc and
README):
- `importData`/`importFiles` → `UnsupportedOperationException` (all roles);
- `persistenceManager().target().write`, `updateMetadata`, `updateCurrentObjectId` →
  `UnsupportedOperationException` (replicated roles);
- on readers: every C7 WRITE-class method and the C6 mutators → `ReaderWriteRejectedException`;
- on readers: traversal must run inside `graphBoundary().read(...)` (D-14), because an unsectioned
  traversal may observe a half-applied batch;
- `viewRoots()` omits `"peruncs.replication"`;
- `setRoot` requires a `Lazy`;
- `shutdown()` closes the whole node.

**Deletions.** `ClusterNode.open(NodeOptions)`, `M/api/NodeOptions.java`,
`NodeSettingsSource.env(...)` (replaced in D1).

**Files.** New `M/api/{ClusterStorage,ClusterStorageFoundation}.java`, `M/api/ClusterNode.java`,
`M/node/{NodeAssembly,NodeLifecycle}.java`, `src/main/java/module-info.java`.

**Tests.**
- The porting snippets compile and run.
- Per role, `ClusterStorage.start` reaches `status().ready()`.
- A two-field invariant written in one transaction is never observed half-applied inside
  `graphBoundary().read` on a reader (stress).

### D1 — `NodeConfig`: pluggable configuration (P1; after A3/A9)

**Decision.** Delete `NodeSettingsSource`/`EnvironmentNodeSettingsSource` and all legacy keys. Any
configuration system plugs in through a map or the builder.

```java
public record NodeConfig(NodeRole role, StorageConfig storage, BackupConfig backup,
                         Timeouts timeouts, Limits limits, AeronConfig aeron) {
    public static NodeConfig fromEnvironment();                 // PERUNCS_* process environment
    public static NodeConfig fromMap(Map<String,String> values); // any source: MicroProfile Config, Spring Environment, properties files, …
    public static Builder builder();                            // programmatic
    public record StorageConfig(Path root, long limitBytes, Duration limitCheckInterval, Duration gcInterval) {}
    public record BackupConfig(Path volume, int kept, Duration interval, Duration closeTimeout) {}
    public record Timeouts(Duration graphDrain, Duration applyBudget, Duration offer,
                           Duration recordedPosition, Duration readerStop, Duration reconnect, Duration archiveControl) {}
    public record Limits(int maxValidatedIndexObjects, long bufferPoolRetainedBytes, long applyQueueBytes,
                         int maxTransactionBytes, int chunkSize) {}
    public record AeronConfig(/* the existing AeronSettings records: Topology, Channels, ArchivePolicy, Directories, StorageIdentity */) {}
}
```

- All keys live in one `enum Setting { key, default, parser }`, and the README table is generated
  from it.
- The temporary keys from P1-1 and P1-7 are renamed.
- The merger derives its combined apply/index-refresh deadline from `applyBudget`; it is not a
  separate configuration value.
- **Dependency:** A3 first (it removes the obsolete `replicationStreamName` parameter from
  the writer and reader transport APIs).

### Integration guidance (docs only; D-13)

README section "Using PerunCS inside a container". It is framework-neutral; Helidon, Spring and
others are examples.

1. **Lifecycle.** Start with `ClusterStorage.Foundation()…startNode()` on container start. Close
   `ClusterNode` (idempotent, equivalent to `storageManager().shutdown()`) in the container's
   disposal hook, not a JVM shutdown hook.
2. **Expose** `ClusterStorageManager<T>` as the application's `StorageManager`.
3. **Configure** with `NodeConfig.fromMap(<your framework's config as a map>)` or the builder.
4. **Health**, a recommended mapping:
   - readiness = `status.ready()`;
   - liveness is down on `FAILED` or `RESEED_REQUIRED`;
   - `DEGRADED` (replication) or a failed last backup (C9) are reported but stay live;
   - include `status.role()`.

### C7 — Gate classification of every delegated method (P1; step 10)

**Classes:**
- **WRITE:** `persist`, i.e. admission + exclusive + latch-on-uncertain;
- **READ:** a coordinator read section inside `appSection`;
- **ADMIN:** explicitly ungated;
- **HANDLE:** returns a handle whose use is classified separately;
- **LIFECYCLE;**
- **REJECT.**

| Method(s) | Class | Note |
|-----------|-------|------|
| `store`, `storeAll`×2, `storeRoot`, `setRoot`, `StorageManager.ensureRoot`, `Storer.commit`, PM `store`/`storeAll` | WRITE | `ensureRoot` may initialize and persist the root; readers reject its write path. |
| PM `updateMetadata`, `updateCurrentObjectId`, `GatedPersistenceTarget.write` | REJECT (replicated), WRITE (standalone) | A1.4 |
| `Storer.store`/`storeAll` (buffering), `Storer.clear`/`skip*`/capacity methods | ADMIN | Buffer only; the commit is WRITE |
| `exportAdjacencyData`, `exportTypes`, `issueFullBackup`, `exportChannels`, `Database.getObject`, PM `lookupObject`/`lookupObjectId`/`getObject`/`get`/`collect`×2/`createLoader`/`typeDictionary`, `typeDictionary()`, `viewRoots()`, `databaseName()`, `Database.hasStorage()`/`toIdentifyingString()` | READ | |
| `root()`, `persistenceManager()`, `database()`, `createStorer`/`createLazyStorer`/`createEagerStorer`, PM `createStorer`×4, `createConnection()`/`createBatchStorer()`/`batchStorerBuilder()`, `Database.storage()`/`guaranteeActiveStorage()`, PM `source()`/`target()` | HANDLE | Validity check only; each returned object's methods are classified in their own rows. Connections and Database storage return the facade. Batch-storer creation remains unsupported by the adapter. |
| PM `ensureObjectId*`, `mergeEntries`, `registerLocalRegistry`, `consolidate`, `createRegisterer`, `PersistenceRegisterer.apply`/`register*` | ADMIN (writer, unchanged per C6) / REJECT (readers) | Direct writer delegation preserves Store behavior; these calls do not enter cluster persistence gating. |
| PM `objectRegistry()` | Writer delegates unchanged / REJECT (readers, per C6) | Preserves Eclipse Store's writer behavior. |
| `issueGarbageCollection`, full/budgeted cache, file, and integrity checks, `issueTransactionsLogCleanup`, `issueStorageFlush`, `createStorageStatistics`, `configuration`, `initializationTime`/`initializationDuration`, `operationModeTime`, `isRunning`/`isActive`/`isAcceptingTasks`/`isShuttingDown`/`isStartingUp`/`isShutdown`, `checkAcceptingTasks`, PM `getTargetByteOrder`/`isByteOrderMismatch`/`currentObjectId`/`objectRegistryMonitor`/`Clone`, Storer `clear`/`skip*`/capacity/listener methods | ADMIN | Store-internal housekeeping or read-only probes |
| `accessUsageMarks`, `markUsed`/`markUsedFor`, `unmarkUsedFor`, `markUnused`, `isUsed` | ADMIN | These mutate only the in-memory *usage-mark* set Eclipse Store uses to decide whether a manager is still in use. The writer's `objectRegistry()` preserves Store behavior; readers reject it. |
| `start()`, `shutdown()`, PM `close()` | LIFECYCLE | `start` = idempotent admission check; `shutdown` = node close (C2); PM `close` = no-op |
| `importData`, `importFiles`, `Database.setStorage`, `Database.guaranteeNoActiveStorage` | REJECT | |

**Test spec.**
- **Location:** `T/node/store/GateClassificationTest.java`; the gate table stays beside the
  inventory so an unrecognized Store API method fails the test.
- **Enumeration:** public, non-static, non-bridge, non-synthetic methods exposed by
  `StorageManager`, `StorageConnection`, `Database`, `PersistenceManager`, `Storer`,
  `PersistenceStorer`, and `PersistenceRegisterer`, including inherited defaults. Overloads share a gate by
  method name; role-dependent writer/reader behavior is explicit in the C7 table.
- **Behavior:** `StorageWriteGatingTest` and `ClusterStorageManagerBoundaryTest` exercise write,
  read, reader-rejection, limit, invalidation, and lifecycle paths. Keep these behavioral tests
  focused; do not create dynamic argument factories to invoke every upstream overload.
- **WRITE/REJECT on readers:** assert the exception type.

### Other step-9/10 items

- **A3 (P2).** Replace `ReplicationCursor` and `AeronReplicationCursor` with
  `record ReplicationPosition(UUID clusterId, UUID storeGeneration, long epoch, long recordingId,
  long sequence, long prepareStartPosition, long fencingToken, UUID nodeId)`, the in-memory twin of
  the mark plus the reporting node id needed to create reader watermarks and identify backup authors.
  Keep the small transport `NoOp` implementations: they provide a null-free implementation for
  the supported `none` transport, and A5 role assembly is withdrawn. Remove runtime transport-id
  checks and `replicationStreamName` (it selects nothing: topology settings already select the one
  stream this transport owns). Keep the transport-name parser at the external settings boundary
  until D1 replaces string-based settings with typed configuration.
- **A5 / S4 (P1).** **A5 withdrawn:** separate role classes would duplicate the shared
  lazy-resource and close graph for a few explicit role branches. The role is parsed once in the
  collaborator graph and reused throughout startup; preserve the single close sequencer. **S4:**
  keep the real-reader dictionary assertion for the replication-mark type before the first import.
  The duplicate reseed gate and field-by-field `StorageConfiguration` copy are removed.
- **A6 (P2).** One flat `ReplicationStatus` record is returned for each public status read; `NodeStatus`
  already carries the configured `NodeRole`. Unknown metrics use `-1` and `has…()` accessors.
  Delete the `StorageNodeHealthCheck` and `StorageNodeControl` metric defaults and `ReplicationMetrics`.
  Do not add a second volatile snapshot or `lastApplyEpochMillis`: there is no apply-event timestamp
  source, and a cached copy would not reduce allocations or make independently sampled providers
  atomic. Add that field only if the apply path acquires a real timestamp contract.
- **A7 (P2).** `ClusterStorage` is the single API entry into `node`; `ClusterNode` is an API-only
  interface and its implementation stays in the internal node package. **Status:** implemented in
  rev 56; no other API class imports node assembly.
- **D9 (P2). Status: implemented.** The internal implementation uses focused nested packages:
  `node` for lifecycle and assembly, `node.store`, `node.replication`, `node.aeron`, and
  `node.backup`; storage uses `storage.binary`, `storage.index`, `storage.io`, and
  `storage.aeron` split by wire, position, mark, reader, writer, and config. Each nonempty package
  has `package-info.java`. The module exports only `api` and `errors`. Keeping these narrower
  boundaries avoids coupling Store, transport, and lifecycle internals in broad packages.

---

## 6. Remaining findings (condensed; step 12 unless stated)

- **A8 (optional; for A2b).** Recording discovery **already exists**:
  `AeronReaderTransport.discoverReaderRecordingId` (`M/node/aeron/AeronReaderTransport.java:260-276`)
  lists recordings by `alias=` on the live channel and requires exactly one. The recording id
  setting already defaults to `-1`. The README now describes discovery and the optional recording
  id override. The remaining work is only needed with optional A2b promotion:
  1. the writer includes the epoch in the alias (`alias=<configured>-e<epoch>`), because A2b
     creates a second recording;
  2. readers select the alias for their mark's epoch;
  3. verify the first replayed envelope's `clusterId`/`epoch` against the mark;

  Old recordings without the epoch suffix need a reseed (no compatibility required). **Status:**
  optional A2b work; it is not required by the current single-recording deployment.
- **C8 (P3).** The dictionary parse monitor is moot after P1-7.
- **C9 (P2).** `NodeStatus` gains `BackupStatus backup`: `record BackupStatus(boolean lastFailed,
  String lastFailureMessage /* "" when none */, long lastSuccessEpochMillis /* -1 */)`.
  - It is set by the backup task and cleared (`lastFailed=false`, message `""`) by the next
    successful backup.
  - `ready`/`healthy` no longer depend on backup failure (`M/node/backup/BackupNodeManager.java:210-214`).
  - `ReplicationState.DEGRADED` stays replication-scoped.
  - **Status:** implemented. `BackupStatus` also reports post-publication maintenance failure
    separately; a durable backup remains successful and readiness is unchanged.
- **C12 (P2).** Hard-coded retries move into `NodeConfig`:
  - **Status:** implemented and covered by the final profiles. `StorageBackupManager` uses
    configured stop/poll and retention retry bounds; filesystem publication uses configured retry
    attempts; maintenance and storage-check executors use configured failure/close limits. The dead
    `NativeBufferPool` duplicate was removed; its retained-byte budget already comes from `NodeConfig`.
- **P1-6 (P2).** Fence CRC cleanup is complete with A1. The remaining transaction-CRC and gathering
  `DirectBufferVector[]` offer change is **deferred until performance comparison**: it can remove the
  reusable staging copy, but needs reusable buffer/vector scratch sized to the changing Store buffer
  count and may trade that copy for more vector traversal. Keep the current allocation-bounded staging
  path until a same-work comparison can select the faster implementation.
- **P1-10 (P1).** Upload staging uses `FileChannel.transferTo` with a 1 MiB direct buffer only
  when the platform reports no progress. Backup creation computes CRC32C while streaming Store
  files into the ZIP and writes the identity sidecar afterward, eliminating the separate digest
  pass. Backup and integration coverage pass. Directory backups remain optional; no
  manifest after A1.
- **P1-11 (P3).** **Implemented:** `StorageUsageGauge` reads the latest scheduled snapshot without
  spawning virtual threads; one five-second maintenance task owns directory measurement, and the
  storage-limit task consumes the cached bytes. Final profile coverage passes.
- **P1-12 (P2).** Threads per reader after A1 + P1-7: poll, apply, watermark, maintenance, storage
  checks, retention. **Deferred pending thread/latency profiling.** The reader poll loop and watermark
  drain have different idle, retry, and close pacing; combining them without evidence would couple
  replay progress to control-stream work. Apply stays separate because it can block on Store work.
- **S1 (P2; D-22).** Retain same-name publication checks in
  `M/node/backup/FilesystemVolumeBackupBackend.java`: a crash retry is idempotent only for the
  same archive identity, and a conflicting ID must not replace a durable backup. The ZIP digest
  runs outside `.publish.lock`; a file-stamp check retries if the destination changes before the
  locked publish. Removing this path would reintroduce data loss or hold the shared lock while
  hashing a potentially large archive.
- **S3, S5, S6.**
  - S3: **Implemented:** one `section(lock, invalidateOnFailure, supplier)` helper in
    `StorageGraphCoordinator`; final profile coverage passes.
  - S5: **Withdrawn.** Envelope, watermark, retention state, and backup identity have different
    framing, checksums, and mutation rules. A generic codec would add a format switch and shared
    versioning policy without eliminating meaningful code or changing a compatibility contract.
    Keep each small codec beside its owning format until a real shared schema emerges.
  - S6: **Implemented.** `AeronArchiveReader` owns stop, reconnect, and disposal transitions;
    terminal stop outcomes are sticky and subscription disposal remains retryable after a timeout.
    `AeronReaderLifecycleTest` covers bounded stop/disposal and retry cases; final profile coverage
    passes.
- **D2–D8, D10.**
  - Single-implementation interfaces become final classes. **D2 progress:** `StorageUsageGauge` and
  `StorageNodeHealthCheck` are concrete classes; `StorageNodeManager` is one concrete package-private
  manager; the `NodeAssembly` interface is removed and its builder/start bridge now live on
  `NodeLifecycle`; and the one-method `StorageSizeValidation` seam uses `BooleanSupplier`. The
  remaining interfaces were reviewed as API boundaries, role-selected strategies, or narrow
  transport/Store contracts; no further single-implementation seam was removed in this sweep.
  - Fold static-only types.
  - D4: sealed `NodeException`, final leaves, `outcome()` derived from the types
    (`WriteRejectedException` → `RETRYABLE`, `ReplicationPendingException` → `PENDING`,
    `ReseedRequiredException` → `RESEED_REQUIRED`, all other failures → `FAILED`), move
    `ReplicationPositionUnavailableException` internal, remove the unread
    `ReplicationUnavailableException.errorCode()` and its provider-code constructor (removed
    2026-09-28); the supported message/cause constructors stay public. **Status:** implemented and
    pinned by `NodeExceptionOutcomeTest`.
  - D5–D8 decided (D-16..D-19, D-15).
  - D10: **implemented.** `TransactionAssembler` and `AeronReplicationPublisher` now accept one
    immutable construction record; `EnvelopeFramer` uses a publisher-wide configuration record
    and primitive per-transaction values. `Delivery` uses `prepareCommit` (5 arguments) and
    `prepareAbort` (2 arguments) on its existing reusable instance instead of allocating one record
    per transaction.
- **J2 (P2).** **Audited.** 126 source lines contain `synchronized` (2026-09-29). The remaining
  monitors protect compound state transitions in the publisher, Archive reader, watermark channel,
  lifecycle, and apply worker. The hot publisher monitor is released before offer back-pressure and
  durability waits; Archive `publishTransaction` is test-only, while its other synchronized work is
  control/close. Replacing these with atomics would split invariants or add retry loops without
  measured contention, so keep the short state sections and revisit only with lock-profile evidence.
- **J3–J6.**
  - **J3: Implemented.** No fully-qualified PerunCS type references remain in production or test
    bodies; package declarations and imports are excluded.
  - **J4: Implemented.** Node close uses one retryable `CloseSequencer`.
  - **J5: Implemented.** Crash and storage-file failure injection use one `FaultInjection` seam.
  - **J6: Withdrawn.** Startup collaborators have dependency-ordered construction, and close stages
    must run in dependency order. There is no independent startup/close fan-out for
    `StructuredTaskScope` to join; parallelizing these stages would change their ownership contract.
    Scoped values remain where they carry actual dynamic context.

### N3 — Archive retention is never triggered in a deployed cluster (P1; found in the rev-5 documentation pass)

**Original evidence (before this change).**
- `ReplicationLogRetention.deleteThrough` has exactly one production caller:
  `StorageBackupManager` (`M/node/backup/StorageBackupManager.java:478-482`).
- `StorageBackupManager` is wired only for the backup-reader (`M/node/NodeAssembly.java`,
  `ensureStorageBackupTaskExecutor` and `NodeLifecycle.startBackupNode`).
- On a backup-reader, `AeronRetentionOwner` returns the "unsupported" retention
  (`M/node/aeron/AeronRetentionOwner.java:43-61`), because retention requires an embedded writer
  with configured retention readers. `advanceRetention` therefore only logs "preserving Archive
  history".
- The writer, which owns the Archive and collects the watermarks, did not call `deleteThrough`;
  only tests and the soak did.

**Consequence.** The writer's Archive grows without bound in production. The quorum and watermark
machinery is inert.

**Fix.** The writer runs retention on its maintenance scheduler. Each pass requests its latest
durable boundary; the retention controller caps deletion at the minimum durable quorum watermark
and complete Archive segments. Incomplete quorums and unavailable retention preserve history.

**Current status (2026-09-30).** `NodeLifecycle` schedules the transport's shared retention pass at the configurable
`PERUNCS_AERON_RETENTION_INTERVAL_MINUTES` cadence (default one minute). Each pass asks
for the latest durable writer position; `AeronArchiveRetention` caps that request at the complete
reader-quorum watermark and purges only complete durable segments. An incomplete quorum or a
quorum that has not crossed a segment preserves history. The backup-reader path remains
unsupported and no control-message protocol was added. Controller quorum-capping tests exist. The
writer-plus-two-reader integration fixture schedules the same maintenance operation used by the
lifecycle and checks lagging and caught-up quorum behavior; the final integration profile passes.

**After A1,** watermarks report the reader's mark (`sequence`, `prepareStartPosition`).

**Acceptance test (passed).** A writer plus 2 retention readers with a small segment length. Drive the
writer's scheduled pass: after both readers pass two segments, it deletes the older segment; while
one reader has not crossed a complete segment, the pass preserves history and maintenance health
stays healthy.

## 7. Security and documentation

- **Protocol constraint.** PerunCS does not provide node authentication or transport encryption;
  neither is required or permitted. No trusted-network acknowledgement setting is part of the
  configuration. The redundant wire framing value is derived from the public cluster UUID; no
  shared secret or nonce credential is configured.

- **SEC1 — implemented.** The default Aeron directory is `<PERUNCS_STORAGE_PATH>/aeron`, beside
  the actual Store directory; driver, Store, and Archive paths may not overlap. `/tmp` remains
  rejected in production mode.
- **SEC2 — implemented.** POSIX backup archives are owner-only (`0600`) and temporary workspaces
  are owner-only (`0700`); filesystems without POSIX attributes retain their native behavior.
  `FilesystemVolumeBackupBackendTest` covers the published archive and workspace modes.
- **SEC3.** Legacy keys removed by D1.
- **SEC4 — implemented.** Error-code checks remain preferred. Aeron 1.53.1 mark-file and control
  response text fallbacks are isolated and version-pinned in source; `AeronRuntimeTest` and
  `AeronControlWarningTest` pin their exact messages.
- **SEC5 — implemented.** Removed the JVM-global publication-mutex registry. Runtime state is
  instance-owned. The only production reflection metadata is the temporary JVector invalidation
  bridge required while Eclipse Store PR #832 is open; test hooks use `ScopedValue`.
- **Docs:** the module descriptor essay is concise and retains the topology and protocol
  constraints. README now has the Eclipse Store porting map, framework-neutral container guidance,
  generated configuration-key table, and node-local live-storage rules. Keep the JVector module
  requirement and do not run a repository-wide formatter (D-28).

---

## 8. Reference corrections (cumulative)

| Earlier claim | Corrected |
|---------------|-----------|
| C10 fix "set before submit, clear in finally" (rev 3) | Wedges close on cancelled-queued tasks; see the §3.C10 state machine |
| A8 "readers must configure the recording id" | Discovery exists (`AeronReaderTransport.java:260-276`); only the README is stale |
| A9 `ClusterFoundation<F>` / exported `ObjectGraphUpdateHandler` (rev 3/4) | Withdrawn (not Data Grid features, D-02/D-11) |
| `NativeBufferPool.java:456` | `:14` |
| `WriterFencingLease` ≈960 LOC, `executeUnderOwnership` 596–657 | 913 LOC; 602–666 |
| 22 lazy holders / ~180 `synchronized` | 21 / 157 lines |
| `fileSyncLevel=0` rejection as new work | Existing (`AeronSettings.java:326-333`); test only |
| A1 "last COMMIT position stored alongside" | Impossible (unknown when the mark is written); `prepareStartPosition` + duplicate skip |
| J6 `ScopedValue` for the C2 counter | Withdrawn |
| "Run a formatter" | Dropped (D-28) |

---

## 9. Test-to-step map

| Step | Tests (defined in the spec blocks) |
|------|------------------------------------|
| 0 | Benchmark baseline |
| 1 | C1 (8 contract cases, including arbitrary failure after recorded ABORT), C2 (`ApplicationSectionDrainTest`, `NodeLifecycleTest`, `AeronApplicationSectionDrainIT` under `-Pintegration`), C3 (4), C5 (existing suite), C6 (2), C10 (3), A4 (probe without `--add-exports` + integration) |
| 2 | Spikes S-1…S-5 |
| 3 | A1 crash matrix (new oracle, including `AeronWriterRecoveryIT`), reader resume, `ReplicationPendingException`, `fileSyncLevel=0` test, C1 re-run |
| 4 | Benchmark compare (D-25) |
| 5 | A2 (3) |
| 6 | P1-1 (3), F1 (fuzz, equivalence, micro-benchmark) |
| 7 | P1-7/N1 (4, checked generation test, 10k randomized materialization stress), F2 (spike gates 1–2, NMT leak test) |
| 8 | C4 read-latency metric |
| 9 | A9 (3), D1 (a parameterized test per `Setting`), A3 (existing suites adapted) |
| 10 | C7 classification test, S4 reader-dictionary assertion, A6 existing suites, standalone status contract; A5 decision withdrawn |
| 11 | J1-a (3) |
| 12 | C9 (status fields), S1 (backup suite), A2b/A8 if taken; `ClusterStorage` contract tests for open/close idempotence on each role (standalone `NOT_CONFIGURED` test added) |
