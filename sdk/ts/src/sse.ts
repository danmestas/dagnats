// A minimal Server-Sent Events parser, by hand so the package needs no
// dependency. It implements the subset the bridge's connect stream uses:
// `event:` and `data:` fields, comments, LF/CRLF/CR line endings. Input
// arrives in arbitrary chunks, so a partial line or event is held until
// the rest comes.

export interface SSEEvent {
  event: string;
  data: string;
}

export class SSEParser {
  private buffer = "";
  private event = "";
  private data: string[] = [];
  private sawField = false;

  /** push feeds a chunk and returns the events it completed. */
  push(chunk: string): SSEEvent[] {
    this.buffer += chunk;
    const out: SSEEvent[] = [];
    for (;;) {
      const end = this.lineEnd();
      if (end === undefined) break;
      const line = this.buffer.slice(0, end.start);
      this.buffer = this.buffer.slice(end.next);
      const done = this.line(line);
      if (done) out.push(done);
    }
    return out;
  }

  /** The end of the first complete line in the buffer, if any. A trailing CR waits for a possible LF. */
  private lineEnd(): { start: number; next: number } | undefined {
    for (let i = 0; i < this.buffer.length; i++) {
      const c = this.buffer[i];
      if (c === "\n") return { start: i, next: i + 1 };
      if (c === "\r") {
        if (i + 1 >= this.buffer.length) return undefined;
        return { start: i, next: this.buffer[i + 1] === "\n" ? i + 2 : i + 1 };
      }
    }
    return undefined;
  }

  private line(line: string): SSEEvent | undefined {
    if (line === "") {
      if (!this.sawField) return undefined;
      const ev = { event: this.event === "" ? "message" : this.event, data: this.data.join("\n") };
      this.event = "";
      this.data = [];
      this.sawField = false;
      return ev;
    }
    if (line.startsWith(":")) return undefined;
    const colon = line.indexOf(":");
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") {
      this.event = value;
      this.sawField = true;
    } else if (field === "data") {
      this.data.push(value);
      this.sawField = true;
    }
    return undefined;
  }
}
