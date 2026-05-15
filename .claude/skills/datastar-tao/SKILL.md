---
name: datastar-tao
description: Use when building hypermedia-driven web applications, server-rendered UIs, or
any frontend where the backend should own state. Use when choosing between SPA
and server-driven architecture. Use when reviewing frontend code for unnecessary
client-side state, optimistic updates, or client-side routing. Triggers on
requests involving SSE, HTML-over-the-wire, DOM morphing, HTMX, Datastar,
signals, or backend-first frontend design.
---

# The Tao of Datastar

Principles from the [Datastar](https://data-star.dev/) project's philosophy for building maintainable, high-performance web applications. The central thesis: **"The backend is the source of truth. The frontend is a projection of it."**

Every architecture decision must be judged by whether it keeps state on the server, leverages the platform, and avoids reinventing what the browser already does well.

## Core Principles

### 1. Backend Owns State

Most state belongs on the server. The frontend is exposed to the user — it can be tampered with, fall out of sync, or lose context on refresh. The backend is the single source of truth for application state.

**Client-heavy (bad):**

```typescript
// Frontend manages its own "truth" — duplicates backend logic,
// drifts on stale data, breaks on refresh
const [users, setUsers] = useState<User[]>([]);
const [filters, setFilters] = useState({ role: "admin", active: true });
const [sortBy, setSortBy] = useState("name");
const [page, setPage] = useState(1);

// Now you need: sync logic, cache invalidation, optimistic rollback,
// conflict resolution, offline handling...
const filteredUsers = useMemo(() =>
  users
    .filter(u => u.role === filters.role && u.active === filters.active)
    .sort((a, b) => a[sortBy].localeCompare(b[sortBy]))
    .slice((page - 1) * 20, page * 20),
  [users, filters, sortBy, page]
);
```

**Server-owned (good):**

```go
// Backend computes the view, sends the result as HTML
func handleUsers(w http.ResponseWriter, r *http.Request) {
    role := r.URL.Query().Get("role")
    users := db.GetUsers(role, "active", "name", 1)
    // Template renders the final HTML — one source of truth
    tmpl.Execute(w, users)
}
```

```html
<!-- Frontend sends intent, receives rendered result -->
<div data-on-change="$$get('/users?role=admin')">
  <!-- Server returns the filtered, sorted, paginated HTML -->
</div>
```

### 2. Start with the Defaults

The default configuration exists because it covers the majority of use cases. Before changing a default, ask: "Am I solving a real problem, or am I guessing about a future one?" Most custom configurations are premature optimization or cargo-culting from SPA patterns that don't apply.

### 3. Patch, Don't Replace

Since the backend owns truth, it drives the frontend by patching — adding, updating, and removing HTML elements and signals. Send the minimal change, not the entire page. The server knows what changed; let it tell the client exactly what to update.

**Full replacement (bad):**

```typescript
// Replaces entire container — destroys scroll position, focus,
// animations, and any transient UI state
container.innerHTML = await fetch("/users").then(r => r.text());
```

**Targeted patch (good):**

```html
<!-- Server sends only the changed fragment -->
<div id="user-42" data-merge-fragments>
  <span class="status">Active</span>
</div>
<!-- Morphing updates just this element, preserving everything else -->
```

### 4. Use Signals Sparingly

Signals (reactive client-side state) are the escape hatch, not the default. Overusing signals means you're rebuilding a state manager on the frontend — the exact problem backend-first architecture avoids.

**Good uses of signals:**
- Toggling element visibility (accordion, dropdown)
- Binding form inputs before submission
- Transient UI state that has no business meaning (hover, focus)

**Bad uses of signals:**
- Filtering, sorting, or paginating data client-side
- Caching server responses
- Duplicating business logic that belongs on the server

**Rule of thumb:** If the signal's value would matter after a page refresh, it belongs on the backend.

### 5. In Morph We Trust

DOM morphing (updating only the parts of the DOM that actually changed) is the key enabler. It lets you send large HTML fragments — even the entire page — without losing scroll position, focus state, CSS transitions, or form input. This is what makes "just send HTML" viable at scale.

Morphing means you don't need:
- Virtual DOM diffing
- Component-level re-rendering
- Key-based reconciliation
- Manual DOM manipulation

Send the HTML. The morpher handles the rest.

### 6. SSE for Reads, HTTP for Writes (CQRS)

Separate read and write channels. Use Server-Sent Events (SSE) as a single long-lived connection for receiving updates from the backend. Use standard HTTP requests for writes.

```
┌─────────┐         SSE (reads)          ┌─────────┐
│         │ ◄──────────────────────────── │         │
│ Browser │                               │ Server  │
│         │ ──── POST/PUT/DELETE ────────► │         │
└─────────┘      (writes)                 └─────────┘
```

**Why this works:**
- One persistent connection for all real-time updates
- Zero to N events per SSE response — patch elements, patch signals, execute scripts
- Writes are standard HTTP — simple, cacheable, debuggable
- No WebSocket complexity, no bidirectional protocol to manage

### 7. Compression is Your Superpower

HTML + SSE + Brotli compression achieves ratios of 200:1 or higher. Repetitive HTML structures (tables, lists, repeated components) compress spectacularly. This means "send more HTML" is often cheaper on the wire than "send JSON and reconstruct on the client."

Don't optimize by sending less HTML. Optimize by enabling compression and sending the right HTML.

### 8. Use Your Templating Language

Your backend language already has a templating system. Use it. Keep your rendering DRY with partials, layouts, and components on the server side. Don't build a second component system on the frontend.

**Two component systems (bad):**

```typescript
// Server renders a shell, client renders components
// Now you maintain two template systems, two data flows
const UserCard = ({ user }) => (
  <div className="card">
    <h3>{user.name}</h3>
    <p>{user.role}</p>
  </div>
);
```

**One component system (good):**

```go
// Server renders everything — one system, one source of truth
{{ define "user-card" }}
<div class="card" id="user-{{ .ID }}">
  <h3>{{ .Name }}</h3>
  <p>{{ .Role }}</p>
</div>
{{ end }}
```

### 9. Don't Reinvent the Browser

**Page navigation:** Use `<a>` tags. The browser has handled navigation for 30 years. Client-side routing is a complexity tax you pay to solve problems the browser already solved.

**Browser history:** The browser tracks history automatically. The moment you manage history yourself (pushState, replaceState, custom back-button handling), you're adding complexity that fights the platform.

**Forms:** Use `<form>` elements. They handle validation, submission, accessibility, and keyboard interaction out of the box.

**Scroll, focus, viewport:** These are browser-managed state. Don't track them in your application state.

### 10. Honest UIs Over Optimistic Updates

Optimistic updates show the user a lie — "this succeeded" before it actually has. When the backend disagrees, you need rollback logic, conflict resolution, and error recovery that's harder to build correctly than the optimistic update itself.

**Dishonest (bad):**

```typescript
// Show success immediately, pray the server agrees
const handleLike = () => {
  setLiked(true);           // Lie to user
  setCount(count + 1);      // Lie harder
  api.like(postId).catch(() => {
    setLiked(false);        // Awkward rollback
    setCount(count);        // Hope state is consistent
    toast.error("Failed");  // User already scrolled away
  });
};
```

**Honest (good):**

```html
<!-- Show a loading indicator, confirm from server -->
<button data-on-click="$$post('/posts/42/like')"
        data-indicator="#like-spinner">
  Like
  <span id="like-spinner" class="spinner" hidden></span>
</button>
<!-- Server confirms with updated HTML — truth, not hope -->
```

Use loading indicators to show the user that an action is in progress. Confirm success only when the backend confirms it.

### 11. Accessibility is Non-Negotiable

The web should be accessible to everyone. A backend-first architecture has a natural advantage: server-rendered HTML is inherently more accessible than client-rendered JavaScript. But you still need to:

- Use semantic HTML (`<nav>`, `<main>`, `<article>`, `<button>`)
- Apply ARIA attributes where semantic HTML isn't sufficient
- Ensure keyboard navigation works
- Test with screen readers
- Maintain focus management when the DOM morphs

## Red Flags

| Red Flag | What It Means |
|----------|---------------|
| `useState` for server-owned data | State belongs on the backend — you're duplicating truth |
| Client-side filtering/sorting | The server should compute and send the result |
| Client-side routing library | You're reinventing the browser's navigation |
| `pushState` / history management | Let the browser handle history — you're fighting the platform |
| Optimistic updates with rollback | Honest loading indicators are simpler and more correct |
| JSON API consumed by frontend templates | Send HTML, not data — eliminate the client-side render step |
| Multiple state management libraries | Signals are the escape hatch, not the architecture |
| WebSocket for simple request/response | SSE for reads + HTTP for writes covers most cases |
| Frontend form validation duplicating backend | Validate on the server, render errors in the response |
| Custom component framework on the client | Use your backend templating — one component system |

## Reasoning Flow

When this skill is active, follow these steps:

1. **Identify the state** — Where does each piece of state live? Classify as server-owned (business data, user records, permissions) or client-transient (UI toggles, focus, animation).
2. **Move state to the server** — Any state classified as server-owned must be managed on the backend. The frontend receives it as rendered HTML.
3. **Spot platform reinvention** — Are you rebuilding navigation, history, forms, or scroll management? Use the browser's built-in capabilities.
4. **Choose the right channel** — Reads via SSE (real-time, server-pushed). Writes via standard HTTP (POST/PUT/DELETE).
5. **Patch, don't replace** — Send minimal HTML fragments. Let morphing handle the DOM update.
6. **Be honest** — Use loading indicators instead of optimistic updates. Confirm success from the server.
7. **Check accessibility** — Semantic HTML, ARIA, keyboard nav, screen reader compatibility.

## Common Mistakes

| Mistake | Tao Fix |
|---------|---------|
| "We need React for this" | Do you? Server-rendered HTML + morphing handles most UIs without a framework. |
| "Let's cache this on the client" | The server is the cache. Fetch current state when you need it. |
| "We need client-side routing for SPA feel" | Morphing + SSE gives you SPA-like updates without client-side routing. |
| "Let's add optimistic updates for responsiveness" | Loading indicators are honest and simpler. The server confirms truth. |
| "We need WebSockets for real-time" | SSE handles server-to-client updates. HTTP handles client-to-server. Simpler. |
| "Let's add a state management library" | If you need one, you've put too much state on the client. Move it back. |
| "We need to manage browser history" | Let the browser do it. Use `<a>` tags and server redirects. |
| "JSON APIs are more flexible" | Flexible for whom? HTML responses eliminate an entire rendering layer. |
| "Send less data to optimize performance" | Enable Brotli compression. HTML compresses at 200:1. Send the right HTML. |
