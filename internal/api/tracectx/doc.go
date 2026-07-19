// Package tracectx holds the wire-level trace-context tests for the
// control-plane NATS micro handlers. It is a separate package -- and so
// a separate test binary -- on purpose: those tests must install a real
// SDK TracerProvider globally, which makes every api.Service span
// recording. Sibling tests inside internal/api assert that an untraced
// request persists an EMPTY trace_id, which only holds while the binary
// has the noop tracer. Isolating the recorder here keeps both sets of
// assertions honest without either weakening the other.
//
// The two sibling tests are TestNATSAPIStartRunPropagatesTraceContext
// (its untraced half) and TestGetRunResponseNoTrace. Both are latent
// tripwires: anyone who installs a TracerProvider inside the
// internal/api test binary will fail them, because a recording root
// span mints a real trace ID that is then legitimately persisted --
// which is what production does, since it calls InitTelemetry. Fix them
// by asserting the untraced trace ID DIFFERS from the traced one rather
// than by weakening the recorder.
package tracectx
