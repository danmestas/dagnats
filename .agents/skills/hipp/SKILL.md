---
name: hipp
description: Use when designing libraries, modules, or data layers that must be simple, reliable, and self-contained. Use when choosing between embedded vs server-based solutions. Use when reviewing code for unnecessary complexity, dependencies, or configuration. Triggers on requests involving zero-config design, embedded systems, long-term maintainability, or first-principles thinking.
---

# Hipp's Philosophy of Software Design

Principles from Dr. D. Richard Hipp, creator of [SQLite](https://sqlite.org/), [Fossil SCM](https://fossil-scm.org/), and Lemon parser. Hipp built SQLite in 2000 because he needed a database that "just worked" on a U.S. Navy battleship — zero configuration, no server, direct-to-disk, ultra-reliable, and dead simple.

The central thesis: **"Small. Fast. Reliable. Choose any three."** Every decision must be judged by whether it keeps the system smaller, more reliable, more independent, and simpler for the long haul.

## Core Principles

### 1. Simplicity is Supreme

Design so the solution "just works" with zero configuration, zero maintenance, and minimal cognitive load. Complexity is the enemy.

**Config-heavy (bad):**

```python
# Caller must understand config schema, file paths, env vars, defaults
app = Application(
    config_path="config/app.yaml",
    env_prefix="MYAPP_",
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    db_url=os.getenv("DATABASE_URL"),
    cache_backend=os.getenv("CACHE_BACKEND", "redis"),
    feature_flags_file="config/features.json",
)
```

```typescript
// Caller must understand config schema, file paths, env vars, defaults
const app = new Application({
  configPath: "config/app.yaml",
  envPrefix: "MYAPP_",
  logLevel: process.env.LOG_LEVEL ?? "INFO",
  dbUrl: process.env.DATABASE_URL!,
  cacheBackend: process.env.CACHE_BACKEND ?? "redis",
  featureFlagsFile: "config/features.json",
});
```

**Zero-config (good):**

```python
# Just works. Sensible defaults derived from the problem.
app = Application("myapp.db")
```

```typescript
// Just works. Sensible defaults derived from the problem.
const app = new Application("myapp.db");
```

### 2. Reliability Through Ruthless Testing

Prioritize correctness above all. Aviation-grade testing — 100% coverage, regression suites that prove invariants. Bugs should "dry up" after release. Testing enables fearless refactoring.

### 3. Economy & Independence

Keep it small, compact, portable, and self-contained. Avoid servers, heavy dependencies, or runtimes. Embeddable is almost always better.

**Client-server (bad when embedded suffices):**

```python
# Requires a running server, network, connection pooling, auth, config
import psycopg2
conn = psycopg2.connect(
    host="db.internal", port=5432,
    dbname="app", user="admin", password=os.getenv("DB_PASS")
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
```

**Embedded (good):**

```python
# No server, no network, no auth, no config. Just a file.
import sqlite3
conn = sqlite3.connect("app.db")
conn.execute("SELECT * FROM users WHERE id = ?", (user_id,))
```

```typescript
// No server, no network, no auth, no config. Just a file.
import Database from "better-sqlite3";
const db = new Database("app.db");
db.prepare("SELECT * FROM users WHERE id = ?").get(userId);
```

### 4. First-Principles Thinking

Derive the design from the actual problem. Ignore "what experts do" if it adds unnecessary complexity. Solve your problem, not someone else's.

### 5. Resist Feature Creep

Say "no" early and often. Every added feature must justify itself or it gets cut. A smaller API is easier to learn, test, and maintain.

**Kitchen-sink (bad):**

```python
class DataStore:
    def get(self, key): ...
    def put(self, key, value): ...
    def delete(self, key): ...
    def batch_get(self, keys): ...
    def batch_put(self, items): ...
    def watch(self, key, callback): ...
    def subscribe(self, pattern, callback): ...
    def export_to_csv(self, path): ...
    def import_from_json(self, path): ...
    def replicate_to(self, remote): ...
    def set_ttl(self, key, seconds): ...
    def get_stats(self): ...
```

**Minimal (good):**

```python
class DataStore:
    def get(self, key): ...
    def put(self, key, value): ...
    def delete(self, key): ...
```

### 6. Long-Term Viability

Write code readable and maintainable by people not yet born. Plan for decades of support. Avoid trendy abstractions that won't age well.

### 7. Solve More Problems Than You Create

The finished design should reduce overall system complexity, not introduce new ones. No config hell, no hidden state, no fragile abstractions.

### 8. Flexible Where It Helps, Strict Where It Matters

Make the common case simple; make the dangerous case impossible or loudly obvious. Dynamic behavior is a feature when it removes unnecessary rigidity.

**Rigid (bad when flexibility helps):**

```python
# Forces caller to convert types before storing — unnecessary friction
def store_setting(key: str, value: str) -> None:
    # Everything must be a string, caller must serialize/deserialize
    ...

store_setting("timeout", str(30))  # Annoying
store_setting("debug", str(True))  # Annoying
```

**Flexible (good):**

```python
# Accepts what makes sense, stores it naturally
def store_setting(key: str, value: str | int | float | bool) -> None:
    # Coerces naturally, like SQLite's type affinity
    ...

store_setting("timeout", 30)    # Just works
store_setting("debug", True)    # Just works
```

## Red Flags

Complexity symptoms to call out during review, design, or implementation.

| Red Flag | What It Means |
|----------|---------------|
| Configuration files or setup rituals | Should "just work" — zero-config is the goal |
| Server-based when embedded would suffice | Unnecessary complexity, deployment burden, and failure modes |
| "Might need it later" features | Feature creep — cut it until proven necessary |
| Over-engineered abstractions | OO patterns or layers that add complexity without solving the actual problem |
| Untested or lightly-tested paths | Reliability failure — every path must be proven correct |
| External dependencies that break portability | Independence violation — can it run anywhere without help? |
| Designs that won't be readable in 10+ years | Long-term viability failure |
| Hidden state or implicit behavior | Creates more problems than it solves — make behavior obvious |

## Reasoning Flow

When this skill is active, follow these steps:

1. **Restate the core problem** — What must "just work"? One sentence.
2. **Spot complexity** — Scan for red flags: config, dependencies, servers, bloat, fragility.
3. **Design from first principles** — Derive a minimal solution directly from the problem, not from conventions.
4. **Design it twice** — Sketch the obvious/complex approach and a Hipp-simple/reliable alternative. Score each on: small, fast, reliable, independent, long-term viable.
5. **Pick and justify** — Choose the simplest, most reliable option. Reference specific principles by name.
6. **Implement or recommend** — Provide concrete code, module structure, or architecture with test strategy.
7. **Comment only the why** — Comments for contracts and non-obvious decisions. Good design should be self-evident.

## Common Mistakes

| Mistake | Hipp Fix |
|---------|----------|
| "Let's add a config file for that" | Zero-config is the goal. Derive sensible defaults from the problem. |
| "We need a server for this" | Can it be embedded? If yes, skip the server. |
| "Let's add this feature while we're here" | Does it justify itself? If not, cut it. Say no early and often. |
| "Use the standard framework/ORM/pattern" | Does it solve your actual problem, or someone else's? Think from first principles. |
| "We can test that later" | Test now. 100% coverage enables fearless refactoring. Untested code is broken code you haven't caught yet. |
| "Add this dependency, it saves time" | Every dependency is a portability risk and a reliability risk. Can you write the 50 lines yourself? |
| "Make it flexible for future use cases" | Flexibility you don't need today is complexity you pay for forever. |
