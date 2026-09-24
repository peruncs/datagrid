Review @README.md and its module-info.java record for the general outline of the Aeron datagrid implementation

## Code and documentation navigation
For code intelligence (symbols, call chains, architecture, impact), use the codebase-memory-mcp - check `http://localhost:7995/mcp`.


## General code rules - items to investigate or fix:

1. Use all modern Java features for the java version specified in the build framework, including preview features such as: Virtual threads, Patern matching, ScopedValue, StructuredTaskScope, LazyConstant. Do not use ThreadLocal.
2. Follow good OOP design - map the Java entities map to expected APIs and domain concepts. Are Java entities and methods properly named, reflected
   on their purpose?
3. Prefer java records, immutable entities, modern functional style java code. Use Optional only for method input params.
4. Functional gaps and architectural design issues.
5. Opportunities to further simplify both the design and the code, make it DRY, cleaner and re-use as much as possible!
6. Signs of overengineering. Ask if this feature/code/functionality is really needed, remove or simplify it.
7. Ask yourself - how would you have approached or implemented this differently - and pursue the better approach?
8. Single-use methods that could be inlined. Java entities with 1-2 static methods that should be folded into stronger entities.
9. Proper use of AutoClosable with try/catch resources managemnt in Java. Exceptiong handlig in genral, sallowed exceptions.
10. Minimal Java entinties and methods visibility surface (do not use "public" without reason)
11. Proper package structure -  naming, avoid unneccessry cross-package placement,  unnecessary public visibility. Add package-info.java javadocs. Do not throw everything in one big "god" package.
12. Beware of Java "god" obects/interafaces/records. When possible, break them into smaller focused entities, that are easier to junit test and reason about.
13. Avoid Java reflection unless absolutely necessary.
14. Javadocs at all levels - module, package and individual Java entities. Use simple narrative suitable for humans, less jargon, first sentence is the most important.
15. Avoid using fully qualified names FQN where sesnible imports can make the code more compact and better to read.
16. Avoid methods with more than 5 arguments - consider replacing them Java record inputs, especially for public apis. But avoid watch out for GC pressure and memory unefficiencies, if the code is on the hot path!
17. Conside Builder pattern for records and classes with complex structure and constructors,  and many fields, to make the code less error prone and more readable.
18. An interface with staic methods only should be converted to a final class with private construtor and static methods.
19. Any security gaps.
20. Any performance issues.
21. Any threading, races, deadlocks, TOCTOU and data corruption issues.
22. Robustness in face of network issues, configurable retrys.
23. Proper exception design, handling, propagation and reporting.
24. Correct and informative javadocs, including javadocs for packages (package-info.java) and modules (module-info.java)
25. Add ample well-documented junit test and simulation coverage.
26. Have we looked at the Aeron examples and cookbook for best practices? Have you looked at the Eclipse Store /Serializer tests? Does the implementation follow them?
27. Prefer use  of Agrona and Eclipse Serializer and Eclipse Store thread utils (LockedExecutor, StripeLockedExecutor) over synchronized.
28. Cluster constraints are strictly obeyed: 1-writer/N-reader nodes. No node authentication features, no transport level encryption.
29. Memory inefficiencies when packing data in Aeron and Eclipse Serializer. Both formats use memory mapped files/ off-the-heap apis, so we want to avoid allocating objects (even temporary) on the JVM heap.
30. Avoid using unsafe/internal jdk apis for accessing off-the-heap memory. 
31. Use native memory/byte bufffer utilities from Aeron/Agrona or Ecipse Serializer, if you can.
32. Make sure embedded Lucene and JVector indexes are tested and part of the implementation.
33. Correctness of the  cluster code
34. Cluster performance - liveliness, throughput, threading.


### If asked for review only

1. Do not run builds or junit tests - your task is only to do code analysis and review.
2. Do not modify any code - this is just a static analysis request.
3. Do not mention what is done correctly and well - there is no value in it.
4. When reporting you findings, be very specific and always offer your detailed recommendations for what and how you woudl do it instead.

### If asked to change code:

Change the code aggressively, no code or data backward compatibility required!

## Git policy

Under any circumstances, Do not perform ANY git mutations, do not restore code form Git , do not lose code! 
Do git mutations only when asked explicitly, then use short, 1-liner commit mesaages.

## External libraries (`GITHUB_ROOT`)

`GITHUB_ROOT` is a **required** environment variable on each developer's
machine. It points to the directory containing `owner/repository` checkouts,
for example:

```bash
export GITHUB_ROOT="$HOME/projects/github"
```

Here are the reference source code directories to borrow from:

`$GITHUB_ROOT/aeron-io`
`$GITHUB_ROOT/eclipse-store`
`$GITHUB_ROOT/eclipse-serializer`
`$GITHUB_ROOT/bhf/aeron-cache`

## Local JVM Performance Monitoring

## 1. Local JVM CLI tools

| Tool | Purpose |
|---|---|
| `jps -lvm` | Find running JVMs (PID + main class/args) |
| `jstat -gcutil <pid> 1000` | GC stats, polled |
| `jstack <pid>` / `jcmd <pid> Thread.print` | Thread dump (deadlocks, stuck threads) |
| `jmap -heap` / `-histo` / `-dump` | Heap summary, class histogram, heap dump |
| `jcmd <pid> help` | The modern all-in-one tool — flags, GC, threads, JFR control |
| `jconsole <pid>` | GUI, live attach |

Requires same user (or root) as the target JVM, and attach mechanism not disabled.

## 2. JFR (Java Flight Recorder)

Built into the JDK (11+, backported to 8u262+), low-overhead, best for deep profiling:
```bash
jcmd <pid> JFR.start name=rec duration=60s filename=recording.jfr
jcmd <pid> JFR.dump name=rec filename=snapshot.jfr
jcmd <pid> JFR.stop name=rec
```
Or launch with recording enabled: `-XX:StartFlightRecording=disk=true,maxsize=250M,maxage=1d,filename=continuous.jfr` — safe to leave running continuously in production (~1% overhead).

Analyze with `jfr summary`/`jfr print` (CLI) or **JDK Mission Control (JMC)** for flamegraphs, allocation/lock analysis.

## 3. How an AI agent interprets `.jfr` files

`.jfr` is **binary and can be huge** — an agent must convert and aggregate before it ever hits the LLM context:
- `jfr summary` → event type/counts overview (cheap first pass)
- `jfr print --json --events <type>` → structured, filterable dump
- `jdk.jfr.consumer.RecordingFile` (Java API) → programmatic iteration for custom aggregation (hot methods, GC pauses, allocation histograms)
- JMC's core libraries (headless) → run the same "automated analysis" rules JMC's GUI uses, get pre-digested findings
- `jfrconv`/async-profiler → collapsed stacks / flamegraphs — the most LLM-friendly text format for "what's slow" questions

**Key principle**: write code to count/aggregate/diff, and only pass the LLM a compact summary (top-N tables, time-bucketed stats) — never raw millions-of-events dumps.

## 4. How an AI agent interprets `jcmd` output

`jcmd` output is **already plain text**, so no binary decoding — but formats are inconsistent per subcommand, so an agent needs per-command parsing:
- `VM.flags`, `VM.system_properties` → trivial key/value parsing, small enough to pass to LLM directly
- `GC.heap_info` → small, safe to pass through; parse numbers if tracking trends over time
- `Thread.print` → hardest one: split per-thread, aggregate by state + top stack frame instead of dumping all threads; **hardcode a check for `Found one Java-level deadlock`** rather than relying on the LLM to spot it
- `GC.class_histogram` → clean tabular data; for leak-hunting, diff two histograms over time in code, not via LLM reasoning over two big tables

**Key principle** (same as JFR): let code do exact counting/diffing/pattern matching for high-volume or safety-critical signals (deadlocks, thresholds); hand the LLM only the compact result.

## Overall takeaway
For both `.jfr` and `jcmd`, the pattern is identical: **deterministic code does extraction/aggregation/pattern-matching; the LLM reasons over a small, structured summary** — never raw dumps. JFR needs this because of binary format + volume; `jcmd` needs it mainly for high-volume text outputs (`Thread.print`, `GC.class_histogram`) and for signals worth catching reliably (deadlocks) rather than probabilistically.
