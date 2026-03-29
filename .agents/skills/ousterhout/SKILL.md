---
name: ousterhout
description: Use when designing modules, classes, APIs, or system architecture. Use when reviewing or refactoring code for complexity. Use when choosing between implementation approaches. Triggers on requests involving abstraction design, interface simplicity, information hiding, or reducing cognitive load.
---

# Ousterhout's Philosophy of Software Design

Principles from John Ousterhout's [A Philosophy of Software Design](https://web.stanford.edu/~ouster/cgi-bin/book.php) (2018). The central thesis: **"The primary goal of software design is to minimize complexity."**

Every suggestion, refactor, and architecture decision must be judged by whether it reduces or increases cognitive load for future readers and modifiers of the code.

## Core Principles

### 1. Deep Modules

Maximize the ratio of functionality to interface complexity. A small, obvious interface that hides rich, powerful behavior (deep) beats a large interface with trivial behavior (shallow).

**Shallow (bad):**

```python
class UserValidator:
    def validate_email(self, email: str) -> bool: ...
    def validate_name(self, name: str) -> bool: ...
    def validate_age(self, age: int) -> bool: ...
    def validate_address(self, address: str) -> bool: ...
    def validate_phone(self, phone: str) -> bool: ...
```

```typescript
class UserValidator {
  validateEmail(email: string): boolean { /* 2 lines */ }
  validateName(name: string): boolean { /* 2 lines */ }
  validateAge(age: number): boolean { /* 2 lines */ }
  validateAddress(address: string): boolean { /* 2 lines */ }
  validatePhone(phone: string): boolean { /* 2 lines */ }
}
```

**Deep (good):**

```python
class User:
    @classmethod
    def create(cls, raw_data: dict) -> "User":
        """Validates, normalizes, and constructs a User.
        Raises ValueError with specific messages on invalid input."""
        # All validation and normalization hidden here
        ...
        return cls(...)
```

```typescript
class User {
  static create(rawData: Record<string, unknown>): User {
    // All validation and normalization hidden here
    // Throws with specific messages on invalid input
    return new User(/* validated fields */);
  }
}
```

### 2. Strategic Programming

Invest extra time now to keep the design simple for the future. Tactical "quick-and-dirty" changes are only acceptable when explicitly justified and time-boxed by the user.

### 3. Information Hiding

Hide every implementation detail not essential to the module's user. The interface should be obvious; the internals opaque.

**Leaking (bad):**

```python
class Cache:
    def __init__(self):
        self._store: dict[str, tuple[float, bytes]] = {}

    def get_store(self) -> dict[str, tuple[float, bytes]]:
        return self._store
```

```typescript
class Cache {
  private store: Map<string, [number, Uint8Array]> = new Map();
  getStore(): Map<string, [number, Uint8Array]> { return this.store; }
}
```

**Hidden (good):**

```python
class Cache:
    def get(self, key: str) -> bytes | None: ...
    def put(self, key: str, value: bytes, ttl_seconds: int = 300) -> None: ...
```

```typescript
class Cache {
  get(key: string): Uint8Array | null { ... }
  put(key: string, value: Uint8Array, ttlSeconds = 300): void { ... }
}
```

### 4. Minimize Cognitive Load

Reduce the knowledge a developer must hold in their head to use or modify the code. Eliminate "unknown unknowns" — things a developer needs to know but has no way of discovering from the code itself.

### 5. Pull Complexity Downward

Push messy details into lower layers so higher layers stay clean and simple. A module's author should suffer complexity so its users don't have to.

**Pushed up (bad):**

```python
# Caller must know about retries, backoff, headers, serialization
response = http.post(
    url,
    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    body=json.dumps(payload),
    timeout=30,
    retries=3,
    backoff_factor=0.5,
)
result = json.loads(response.body)
```

**Pulled down (good):**

```python
# Module handles retries, serialization, auth internally
result = api_client.send(payload)
```

### 6. Define Errors Out of Existence

When possible, make error cases impossible rather than requiring callers to handle them.

**Error-prone (bad):**

```python
file.seek(offset)  # What if offset is past EOF? Caller must check.
data = file.read(n)  # What if n bytes aren't available? Caller must check.
```

**Defined away (good):**

```python
# read_at handles out-of-bounds by returning whatever is available (possibly empty)
# No error for the caller to handle — the behavior is always valid
data = file.read_at(offset, n)
```

### 7. Design It Twice

Before committing to a design, sketch a second, simpler alternative. Evaluate both against the principles above — interface simplicity, implementation depth, future maintenance cost, cognitive load — and pick the one that minimizes long-term complexity.

## Red Flags

Complexity symptoms to call out during review, design, or implementation.

| Red Flag | What It Means |
|----------|---------------|
| Shallow classes/methods | Tiny interface + tiny behavior — adds abstraction cost without hiding anything |
| Getters/setters that leak state | Internal representation exposed through the interface — information hiding failure |
| God objects / massive config | One thing knows too much — cognitive load and change amplification |
| Change amplification | One logical change requires edits in many places — abstraction boundaries are wrong |
| Obscure dependencies | Module behavior depends on something non-obvious — unknown unknowns |
| Over-generalization | "We might need this someday" — complexity added for hypothetical requirements |
| Pass-through methods | Method that does nothing but delegate to another — shallow by definition |
| Conjoined methods | Two methods that can't be understood independently — hidden coupling |

## Reasoning Flow

When this skill is active, follow these steps:

1. **State the goal** — Summarize the requirement in one sentence.
2. **Spot complexity** — Scan for red flags from the table above (change amplification, cognitive load, leaking abstractions, obscure dependencies).
3. **Design it twice** — Sketch two approaches. Score each on: interface simplicity, implementation depth, future maintenance cost, cognitive load for the next developer.
4. **Pick and justify** — Choose the design that best minimizes long-term complexity. Reference specific principles by name in the justification.
5. **Implement or recommend** — Provide concrete code changes, module structure, or refactoring steps.
6. **Comment only the non-obvious** — Add comments for design decisions and subtle constraints. Never comment what the code already says.

## Common Mistakes

| Mistake | Ousterhout Fix |
|---------|----------------|
| "Let's add a class for that" | Does it hide meaningful complexity? If not, it's a shallow module — merge it. |
| "We should make this configurable" | Configuration pushes complexity upward to the caller. Push it down instead. |
| "Let's handle every edge case at the call site" | Define the error out of existence in the lower layer. |
| "This API needs another parameter" | A growing interface signals a leaking abstraction. Redesign. |
| "I'll clean it up later" | That's tactical programming. Invest the time now or accept the debt explicitly. |
| "One more wrapper layer will fix this" | Pass-through layers add complexity without hiding it. Remove the layer. |
| "The caller should know about this" | If they don't need it to use the module, hide it. |
