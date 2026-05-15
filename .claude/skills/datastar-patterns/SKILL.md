---
name: datastar-patterns
description: Use when implementing UI patterns with Datastar — search, inline editing,
infinite scroll, file upload, validation, bulk operations, polling, lazy
loading, progress indicators, or keyboard shortcuts. Triggers on data-*
attributes, @get/@post/@put/@patch helpers, SSE response formatting, or any "how
do I do X in Datastar" implementation question.
---

# Datastar Patterns

Concrete implementation patterns for Datastar. For architecture decisions, see `datastar-tao`.

## Quick Reference: Attributes

| Attribute | Purpose | Example |
|-----------|---------|---------|
| `data-on:<event>` | Event handler | `data-on:click="@get('/endpoint')"` |
| `data-bind:<signal>` | Two-way input binding | `data-bind:search` |
| `data-signals` | Initialize signals | `data-signals="{count: 0}"` |
| `data-signals__ifmissing` | Initialize only if absent | `data-signals__ifmissing="{count: 0}"` |
| `data-text` | Set element text content | `data-text="$count"` |
| `data-attr:<attr>` | Set HTML attribute reactively | `data-attr:disabled="$_fetching"` |
| `data-effect` | Run expression on signal change | `data-effect="$selections; $_all = $selections.every(Boolean)"` |
| `data-indicator:<signal>` | Set signal true during requests | `data-indicator:_fetching` |
| `data-init` | Run expression on element init | `data-init="@get('/updates')"` |
| `data-on:intersect` | Trigger when element enters viewport | `data-on:intersect="@get('/more')"` |
| `data-on:interval` | Trigger on a timer | `data-on-interval__duration.5s="@get('/poll')"` |

## Quick Reference: Modifiers

| Modifier | Effect | Example |
|----------|--------|---------|
| `__debounce.Nms` | Delay until input pauses | `data-on:input__debounce.200ms` |
| `__throttle.Nms` | Rate-limit execution | `data-on:scroll__throttle.100ms` |
| `__duration.Ns` | Set interval period | `data-on-interval__duration.5s` |
| `__window` | Listen on window, not element | `data-on:keydown__window` |
| `.leading` | Execute immediately on first tick | `data-on-interval__duration.5s.leading` |

## Quick Reference: SSE Response Format

| Event | Data Fields | Purpose |
|-------|-------------|---------|
| `datastar-patch-elements` | `elements`, `selector` (opt), `mode` (opt) | Update DOM fragments |
| `datastar-patch-signals` | `signals` (JSON) | Update client signals |

Merge modes: `outer` (default — replace element by ID), `append`, `prepend`, `remove`.

## Patterns

### Active Search

Debounced input that queries the server as the user types.

```html
<input type="text" placeholder="Search..."
       data-bind:search
       data-on:input__debounce.200ms="@get('/search')" />
<div id="results"><!-- server replaces this --></div>
```

```go
func handleSearch(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query().Get("search")
    results := db.Search(query)
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderTemplate("results", results))
}
```

### Click to Edit

Inline record editing — toggle between view and edit mode.

```html
<!-- View mode -->
<div id="contact">
    <p>Name: John Doe</p>
    <button data-on:click="@get('/contact/edit')"
            data-indicator:_fetching
            data-attr:disabled="$_fetching">Edit</button>
</div>

<!-- Edit mode (returned by server) -->
<div id="contact">
    <input type="text" data-bind:name />
    <button data-on:click="@put('/contact')"
            data-indicator:_fetching
            data-attr:disabled="$_fetching">Save</button>
    <button data-on:click="@get('/contact/cancel')">Cancel</button>
</div>
```

```go
func handleContactEdit(w http.ResponseWriter, r *http.Request) {
    contact := db.GetContact(r)
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderTemplate("contact-edit", contact))
}

func handleContactSave(w http.ResponseWriter, r *http.Request) {
    contact := datastar.ReadSignals[Contact](r)
    db.SaveContact(contact)
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderTemplate("contact-view", contact))
}
```

> Signal data automatically becomes the request body — no `<form>` element needed.

### Inline Validation

Server-side field validation triggered as the user types.

```html
<input type="email" required
       aria-live="polite"
       data-bind:email
       data-on:keydown__debounce.500ms="@post('/validate')" />
<p id="email-error"></p>
```

```go
func handleValidate(w http.ResponseWriter, r *http.Request) {
    form := datastar.ReadSignals[FormData](r)
    errors := validate(form)
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderTemplate("form-errors", errors))
}
```

> Use `aria-live="polite"` so screen readers announce validation updates.

### Bulk Update

Checkbox selection with array signals for batch operations.

```html
<div data-signals__ifmissing="{selections: Array(4).fill(false)}">
    <table>
        <thead><tr>
            <th><input type="checkbox" data-bind:_all
                       data-on:change="$selections = Array(4).fill($_all)"
                       data-effect="$selections; $_all = $selections.every(Boolean)" /></th>
            <th>Name</th><th>Status</th>
        </tr></thead>
        <tbody>
            <tr><td><input type="checkbox" data-bind:selections /></td>
                <td>Joe Smith</td><td>Active</td></tr>
            <!-- more rows -->
        </tbody>
    </table>
    <button data-on:click="@put('/activate')"
            data-indicator:_fetching
            data-attr:disabled="$_fetching">Activate</button>
</div>
```

```go
func handleActivate(w http.ResponseWriter, r *http.Request) {
    signals := datastar.ReadSignals[BulkSignals](r)
    db.ActivateByIndex(signals.Selections)
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderTemplate("table", db.GetAll()))
}
```

> The master checkbox uses `data-effect` to stay in sync with individual row selections.

### Infinite Scroll

Sentinel element triggers loading when it enters the viewport.

```html
<div id="items">
    <!-- existing items -->
    <div id="sentinel"
         data-on:intersect="@get('/items?page=2')">
        Loading...
    </div>
</div>
```

```go
func handleItems(w http.ResponseWriter, r *http.Request) {
    page, _ := strconv.Atoi(r.URL.Query().Get("page"))
    items := db.GetPage(page)
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderTemplate("items", items))
    if db.HasMore(page) {
        sse.PatchElements(renderTemplate("sentinel", page+1))
    }
}
```

> The server returns a new sentinel with the next page number. When no more pages exist, omit the sentinel to stop loading.

### Load More

Button-triggered append with offset tracking.

```html
<div id="list">
    <div>Item 1</div>
</div>
<button id="load-more"
        data-signals:offset="1"
        data-on:click="@get('/more')">Load more</button>
```

```go
func handleMore(w http.ResponseWriter, r *http.Request) {
    offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(
        datastar.WithSelector("#list"),
        datastar.WithMergeMode(datastar.Append),
        renderTemplate("item", db.GetItem(offset)),
    )
    if offset+1 >= maxItems {
        sse.PatchElements(datastar.WithSelector("#load-more"), datastar.WithMergeMode(datastar.Remove))
    } else {
        sse.PatchSignals(fmt.Sprintf(`{"offset": %d}`, offset+1))
    }
}
```

> Use `mode: append` for the list container. Use `mode: remove` on the button when all items are loaded.

### Lazy Tabs

Tab content loads on demand when clicked.

```html
<div id="tabs">
    <div role="tablist">
        <button role="tab" aria-selected="true"
                data-on:click="@get('/tabs/0')">Tab 0</button>
        <button role="tab" aria-selected="false"
                data-on:click="@get('/tabs/1')">Tab 1</button>
    </div>
    <div role="tabpanel" id="tab-content">
        <!-- server replaces this -->
    </div>
</div>
```

```go
func handleTab(w http.ResponseWriter, r *http.Request) {
    tabIndex := chi.URLParam(r, "index")
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(renderTemplate("tab-panel", getTabData(tabIndex)))
    sse.PatchElements(renderTemplate("tab-bar", tabIndex)) // updates aria-selected
}
```

> The server returns both the tab panel content and the updated tab bar with correct `aria-selected` states.

### File Upload

File binding with automatic base64 encoding.

```html
<input type="file" data-bind:files multiple />
<button data-on:click="$files.length && @post('/upload')"
        data-attr:disabled="!$files.length">Upload</button>
```

```go
func handleUpload(w http.ResponseWriter, r *http.Request) {
    signals := datastar.ReadSignals[FileSignals](r)
    for _, file := range signals.Files {
        data, _ := base64.StdEncoding.DecodeString(file.Data)
        os.WriteFile(file.Name, data, 0644)
    }
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(`<div id="status">Upload complete</div>`)
}
```

> Files are automatically base64-encoded by Datastar. Keep files under 1MB.

### Progress Bar

Long-running operation with SSE-streamed progress updates.

```html
<div id="progress"
     data-init="@get('/progress', {openWhenHidden: true})">
    <!-- server streams updates here -->
</div>
```

```go
func handleProgress(w http.ResponseWriter, r *http.Request) {
    sse := datastar.NewSSE(w, r)
    for pct := 0; pct <= 100; pct += 10 {
        sse.PatchElements(fmt.Sprintf(
            `<div id="progress" data-init="@get('/progress', {openWhenHidden: true})">%d%%</div>`,
            pct,
        ))
        time.Sleep(500 * time.Millisecond)
    }
    sse.PatchElements(`<div id="progress">Done!</div>`)
}
```

> Use `{openWhenHidden: true}` to keep the SSE connection alive when the tab is backgrounded.

### Polling

Backend-controlled polling with dynamic frequency.

```html
<div id="time"
     data-on-interval__duration.5s="@get('/poll')">
    <!-- server replaces with current time -->
</div>
```

```go
func handlePoll(w http.ResponseWriter, r *http.Request) {
    now := time.Now()
    duration := 5
    if now.Second() >= 50 {
        duration = 1 // speed up in last 10 seconds
    }
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(fmt.Sprintf(
        `<div id="time" data-on-interval__duration.%ds="@get('/poll')">%s</div>`,
        duration, now.Format("15:04:05"),
    ))
}
```

> The backend controls polling frequency by returning a new `__duration` value in the response. Don't add `.leading` in SSE responses — it causes an immediate re-fire.

### Keyboard Shortcuts

Global keyboard event handling.

```html
<div data-on:keydown__window="
    (evt.key === 'Enter' || (evt.ctrlKey && evt.key === 'l'))
    && (evt.preventDefault(), @get('/action'))
">
</div>
```

> Access the event via `evt`. Use `evt.key` for key names, `evt.ctrlKey`/`evt.shiftKey`/`evt.altKey` for modifiers. Call `evt.preventDefault()` to suppress default browser behavior.

### Event Bubbling (DRY)

Single parent listener instead of N duplicate handlers.

```html
<div data-on:click="
    evt.target.tagName === 'BUTTON'
    && ($id = evt.target.dataset.id)
    && @get('/action')
">
    <button data-id="1">Item 1</button>
    <button data-id="2">Item 2</button>
    <button data-id="3">Item 3</button>
</div>
```

> Attach one listener to the parent. Use `evt.target.dataset.id` to identify which child was clicked. Avoids duplicating the same `data-on:click` on every button.

### Backend Redirect

Server-driven page navigation via SSE.

```html
<button data-on:click="@get('/redirect')">Go</button>
<div id="indicator"></div>
```

```go
func handleRedirect(w http.ResponseWriter, r *http.Request) {
    sse := datastar.NewSSE(w, r)
    sse.PatchElements(`<div id="indicator">Redirecting in 3 seconds...</div>`)
    time.Sleep(3 * time.Second)
    sse.Redirect("/target")
}
```

> Wrap redirects in `setTimeout` for Firefox back-button compatibility if not using the SDK helper.

## Pattern Picker

| Task | Pattern |
|------|---------|
| User types, results update live | Active Search |
| Show a record, edit inline | Click to Edit |
| Validate fields as user types | Inline Validation |
| Select multiple rows, act on them | Bulk Update |
| Content loads as user scrolls down | Infinite Scroll |
| "Load more" button appends items | Load More |
| Tab content loads on demand | Lazy Tabs |
| Upload files to the server | File Upload |
| Long-running operation with progress | Progress Bar |
| Periodic data refresh | Polling |
| Global keyboard shortcuts | Keyboard Shortcuts |
| Many buttons with the same action | Event Bubbling (DRY) |
| Navigate user from the backend | Backend Redirect |
