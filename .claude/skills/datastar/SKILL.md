---
name: datastar
description: Use when building web applications with Datastar — the hypermedia framework that
drives frontend reactivity from the backend using HTML data-* attributes and
Server-Sent Events. Triggers on Datastar, data-star, SSE-driven UI, hypermedia
framework, backend-driven frontend, data-signals, data-on, PatchElements, or any
Go/Python web app using Datastar SDKs.
---

# Datastar — Hypermedia-First Reactive Framework

Datastar is a lightweight framework for building reactive, real-time web applications by driving frontend logic and state from the backend using HTML `data-*` attributes and Server-Sent Events (SSE). No client-side JavaScript framework required.

**Core idea:** The backend sends HTML fragments and state updates via SSE. The frontend declares behavior with `data-*` attributes. No virtual DOM, no build step, no npm.

## How It Works

```
Browser                          Server (Go)
  │                                 │
  │  data-on:click="@get('/api')"   │
  │ ──────────────────────────────> │
  │                                 │  ReadSignals(r, &store)
  │                                 │  sse := NewSSE(w, r)
  │  SSE: datastar-patch-elements   │  sse.PatchElements(html)
  │ <────────────────────────────── │  sse.PatchSignals(json)
  │                                 │
  │  DOM morphs, signals update     │
```

1. User interacts with element → `data-on` fires action (`@get`, `@post`, etc.)
2. Server reads signals from request, processes business logic
3. Server streams SSE events: patch DOM elements, update signals, execute scripts
4. Browser morphs DOM (only changed parts), updates reactive signals

## Go SDK Quick Start

```go
package main

import (
    "fmt"
    "net/http"
    "github.com/starfederation/datastar-go/datastar"
)

type Store struct {
    Count int `json:"count"`
}

func main() {
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        http.ServeFile(w, r, "index.html")
    })

    http.HandleFunc("/increment", func(w http.ResponseWriter, r *http.Request) {
        store := &Store{}
        if err := datastar.ReadSignals(r, store); err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }

        store.Count++
        sse := datastar.NewSSE(w, r)

        sse.PatchElements(
            fmt.Sprintf(`<span id="count">%d</span>`, store.Count),
        )
        sse.MarshalAndPatchSignals(store)
    })

    http.ListenAndServe(":8080", nil)
}
```

```html
<!-- index.html -->
<html>
<head>
    <script type="module" src="https://cdn.jsdelivr.net/npm/@starfederation/datastar"></script>
</head>
<body>
    <div data-signals:count="0">
        <span id="count">0</span>
        <button data-on:click="@get('/increment')">+1</button>
    </div>
</body>
</html>
```

## Go SDK API

### Reading Client State

```go
// ALWAYS read signals BEFORE creating SSE stream
store := &MyStore{}
if err := datastar.ReadSignals(r, store); err != nil {
    http.Error(w, err.Error(), http.StatusBadRequest)
    return
}
```

Signals come from query params (GET) or JSON body (POST/PUT/DELETE). Struct tags use `json:"fieldName"`.

### Creating SSE Stream

```go
sse := datastar.NewSSE(w, r)
```

### Patching DOM Elements

```go
// Replace element (default: outer mode — replaces entire element by ID)
sse.PatchElements(`<div id="content">Hello</div>`)

// Target specific selector with inner mode
sse.PatchElements(`<p>New content</p>`,
    datastar.WithSelectorID("container"),
    datastar.WithModeInner(),
)

// Append to a list
sse.PatchElements(`<li>New item</li>`,
    datastar.WithSelector("ul#items"),
    datastar.WithModeAppend(),
)

// Prepend
sse.PatchElements(html, datastar.WithModePrepend())

// Before/After element
sse.PatchElements(html, datastar.WithSelector("#target"), datastar.WithModeBefore())
sse.PatchElements(html, datastar.WithSelector("#target"), datastar.WithModeAfter())
```

**Merge modes:** `WithModeOuter()` (default), `WithModeInner()`, `WithModeAppend()`, `WithModePrepend()`, `WithModeBefore()`, `WithModeAfter()`

### Updating Client Signals

```go
// From struct
sse.MarshalAndPatchSignals(store)

// From raw JSON
sse.PatchSignals([]byte(`{"count": 42}`))
```

### Removing Elements

```go
sse.RemoveElements("#temporary")
sse.RemoveElement("#single-element")
```

### Executing JavaScript

```go
sse.ExecuteScript(`console.log("Hello from server")`)
sse.ExecuteScript(`import {toast} from './notify.js'; toast("Done!")`,
    datastar.WithExecuteScriptAttributes(`type="module"`),
)
```

### Redirecting

```go
sse.Redirect("/new-page")
```

### Connection State

```go
if !sse.IsClosed() {
    // Connection still alive — safe to send
}
```

## HTML Data Attributes Reference

### Signals & State

| Attribute | Purpose | Example |
|-----------|---------|---------|
| `data-signals` | Define/patch reactive signals | `data-signals:count="0"` |
| `data-computed` | Read-only derived signal | `data-computed:total="$price * $qty"` |
| `data-bind` | Two-way binding to input | `<input data-bind:name />` |
| `data-ref` | Reference to DOM element | `data-ref:myDiv` → `$myDiv.tagName` |

### Rendering

| Attribute | Purpose | Example |
|-----------|---------|---------|
| `data-text` | Bind text content | `data-text="$message"` |
| `data-show` | Toggle visibility | `data-show="$isVisible"` |
| `data-class` | Toggle CSS classes | `data-class:active="$isActive"` |
| `data-attr` | Set any HTML attribute | `data-attr:disabled="$loading"` |
| `data-style` | Set inline styles | `data-style:color="$theme"` |

### Events & Actions

| Attribute | Purpose | Example |
|-----------|---------|---------|
| `data-on` | Event listener | `data-on:click="@post('/save')"` |
| `data-on-intersect` | Viewport intersection | `data-on-intersect="@get('/lazy')"` |
| `data-on-interval` | Recurring timer | `data-on-interval__duration.5s="@get('/poll')"` |
| `data-init` | Run on load | `data-init="@get('/initial-data')"` |
| `data-effect` | Run on signal change | `data-effect="console.log($count)"` |

### Backend Actions

```html
<!-- GET request -->
<button data-on:click="@get('/api/data')">Load</button>

<!-- POST request -->
<button data-on:click="@post('/api/save')">Save</button>

<!-- PUT, PATCH, DELETE -->
<button data-on:click="@put('/api/update')">Update</button>
<button data-on:click="@delete('/api/remove')">Delete</button>
```

All actions automatically send current signals to the server.

### Loading Indicators

```html
<button data-on:click="@get('/slow')"
        data-indicator:loading
        data-attr:disabled="$loading">
    <span data-show="!$loading">Submit</span>
    <span data-show="$loading">Loading...</span>
</button>
```

### Event Modifiers

```html
<!-- Debounce input -->
<input data-on:input__debounce.300ms="@get('/search')" data-bind:query />

<!-- Throttle scroll -->
<div data-on:scroll__throttle.100ms="handleScroll()"></div>

<!-- Once only -->
<div data-on-intersect__once="@get('/lazy-load')"></div>

<!-- Prevent default -->
<form data-on:submit__prevent="@post('/save')"></form>

<!-- Window/document events -->
<div data-on:keydown__window="handleKey(evt)"></div>
```

### Persistence

```html
<!-- Persist all signals to localStorage -->
<div data-persist></div>

<!-- Persist specific signals -->
<div data-persist="{include: /theme|lang/}"></div>

<!-- Session storage -->
<div data-persist__session></div>
```

## Patterns

### Search with Debounce

```html
<input data-bind:query
       data-on:input__debounce.300ms="@get('/search')"
       data-indicator:searching
       placeholder="Search..." />
<div data-show="$searching">Searching...</div>
<div id="results"></div>
```

```go
func handleSearch(w http.ResponseWriter, r *http.Request) {
    store := &struct{ Query string `json:"query"` }{}
    datastar.ReadSignals(r, store)

    results := searchDB(store.Query)
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderResults(results),
        datastar.WithSelectorID("results"),
        datastar.WithModeInner(),
    )
}
```

### Infinite Scroll

```html
<div id="list">
    <div>Item 1</div>
</div>
<div data-signals:offset="1"
     data-on-intersect__once="@get('/more')"
     id="load-more">
    Loading more...
</div>
```

```go
func handleMore(w http.ResponseWriter, r *http.Request) {
    store := &struct{ Offset int `json:"offset"` }{}
    datastar.ReadSignals(r, store)

    sse := datastar.NewSSE(w, r)
    sse.PatchElements(
        fmt.Sprintf(`<div>Item %d</div>`, store.Offset+1),
        datastar.WithSelectorID("list"),
        datastar.WithModeAppend(),
    )
    if store.Offset+1 < maxItems {
        sse.PatchSignals([]byte(fmt.Sprintf(`{"offset":%d}`, store.Offset+1)))
    } else {
        sse.RemoveElements("#load-more")
    }
}
```

### Form Submission

```html
<form data-signals:name="''" data-signals:email="''">
    <input data-bind:name placeholder="Name" />
    <input data-bind:email placeholder="Email" />
    <button data-on:click__prevent="@post('/submit')"
            data-indicator:saving
            data-attr:disabled="$saving">
        Save
    </button>
</form>
<div id="message"></div>
```

### Live Updates (Polling)

```html
<div data-on-interval__duration.5s="@get('/status')"
     id="status">
    Checking...
</div>
```

### Tabs

```html
<div data-signals:tab="'home'">
    <button data-on:click="$tab = 'home'" data-class:active="$tab == 'home'">Home</button>
    <button data-on:click="$tab = 'profile'" data-class:active="$tab == 'profile'">Profile</button>

    <div data-show="$tab == 'home'" id="tab-home">Home content</div>
    <div data-show="$tab == 'profile'" id="tab-profile">Profile content</div>
</div>
```

## Key Rules

1. **Read signals BEFORE creating SSE stream.** `ReadSignals(r, &store)` must come before `NewSSE(w, r)`.
2. **Elements need IDs for patching.** Default merge mode is outer — it matches by element ID.
3. **Signals are reactive.** Changing a signal automatically updates all expressions that reference it.
4. **`$` prefix accesses signals.** `$count` reads the `count` signal. All `$name` patterns in expressions become signal reads.
5. **Actions send all signals.** `@get('/api')` automatically includes all signals in the request.
6. **Morphing preserves state.** DOM updates only change what's different — focus, scroll position, form state are preserved.
7. **No build step.** Single `<script>` tag CDN include. No npm, no bundler, no node_modules.
8. **Backend owns the HTML.** Server renders fragments, client morphs them in. This is hypermedia — not JSON APIs + client rendering.
