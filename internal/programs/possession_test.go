// possession_test.go
// Proof of possession (#885): a registration names a public key over an
// anonymous connection, so dagnats holds it pending, with a NATS user
// that can only confirm, until the key's holder confirms over a
// connection logged in with the key. A manifest change of a registered
// program is applied only on confirm. Unconfirmed registrations expire
// and refund their join-key use, freeing the name.
// Methodology: real embedded NATS and a Registrar driven directly;
// Confirm stands in for the authenticated confirm request (the server
// test proves only the key's user can reach it).
package programs

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/program"
)

func registerOnly(t *testing.T, f *fixture, req program.RegisterRequest) (program.Grant, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return f.registrar.Register(ctx, req)
}

func confirm(
	t *testing.T, f *fixture, key string, req program.RegisterRequest,
) (program.Grant, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return f.registrar.Confirm(ctx, key, req)
}

func TestPossession_NewRegistrationIsPendingUntilConfirmed(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	if _, err := registerOnly(t, f, req); err != nil {
		t.Fatalf("Register: %v", err)
	}
	pending := f.record(t, "notify")
	perms := UserPermissions(pending)
	if !pending.Pending || !slices.Equal(perms.Publish.Allow,
		[]string{program.ConfirmSubject(req.PublicKey)}) {
		t.Fatalf("pending record %v, publish %v; want only its confirm subject",
			pending.Pending, perms.Publish.Allow)
	}
	if _, err := confirm(t, f, req.PublicKey, req); err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	confirmed := f.record(t, "notify")
	perms = UserPermissions(confirmed)
	if confirmed.Pending || len(perms.Publish.Allow) < 10 ||
		!slices.Contains(perms.Publish.Allow, program.ConfirmSubject(req.PublicKey)) {
		t.Fatalf("confirmed record %v; want its full permissions plus confirm", confirmed.Pending)
	}
}

func TestPossession_ConfirmChecksTheProvenKey(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	if _, err := registerOnly(t, f, req); err != nil {
		t.Fatalf("Register: %v", err)
	}
	other := publicKey(t)
	_, err := confirm(t, f, other, req)
	wantRefused(t, err, program.RefusedRequestInvalid)
	forged := req
	forged.PublicKey = other
	_, err = confirm(t, f, other, forged)
	wantRefused(t, err, program.RefusedNameTaken)
	if !f.record(t, "notify").Pending {
		t.Fatal("a confirmation by another key promoted the registration")
	}
}

func TestPossession_ManifestChangeAppliesOnlyOnConfirm(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	f.mustRegister(t, req)
	changed := req
	changed.Manifest = cloneManifest(req.Manifest)
	changed.Manifest.TaskTypes = append(changed.Manifest.TaskTypes, "notify.more")
	_, err := registerOnly(t, f, changed)
	if !errors.Is(err, errConfirmRequired) {
		t.Fatalf("anonymous change err = %v, want errConfirmRequired", err)
	}
	if sameManifest(f.record(t, "notify").Manifest, changed.Manifest) {
		t.Fatal("an anonymous request changed a registered program's manifest")
	}
	if _, err := confirm(t, f, req.PublicKey, changed); err != nil {
		t.Fatalf("confirmed change: %v", err)
	}
	if !sameManifest(f.record(t, "notify").Manifest, changed.Manifest) {
		t.Fatal("the confirmed change was not applied")
	}
}

func TestPossession_UnconfirmedRegistrationExpiresAndRefunds(t *testing.T) {
	f := newFixture(t, Config{Open: true, PendingTTL: 50 * time.Millisecond})
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "once", UsesMax: 1})
	squatter := request(t, "notify", plaintext)
	if _, err := registerOnly(t, f, squatter); err != nil {
		t.Fatalf("squat: %v", err)
	}
	if got := f.usesLeft(t, key.ID); got != 0 {
		t.Fatalf("UsesLeft = %d after the pending registration, want 0", got)
	}
	time.Sleep(100 * time.Millisecond)
	_, err := confirm(t, f, squatter.PublicKey, squatter)
	wantRefused(t, err, program.RefusedConfirmExpired)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.registrar.ExpirePending(ctx); err != nil {
		t.Fatalf("ExpirePending: %v", err)
	}
	if _, found, _ := f.registrar.lookup(ctx, "notify"); found {
		t.Fatal("an expired pending registration was not removed")
	}
	if got := f.usesLeft(t, key.ID); got != 1 {
		t.Fatalf("UsesLeft = %d after expiry, want the use refunded", got)
	}
	owner := request(t, "notify", plaintext)
	if _, err := f.register(t, owner); err != nil {
		t.Fatalf("the real owner after the squat expired: %v", err)
	}
}

func TestPossession_ExpiredPendingFreesTheNameOnRegister(t *testing.T) {
	f := newFixture(t, Config{Open: true, PendingTTL: 50 * time.Millisecond})
	if _, err := registerOnly(t, f, request(t, "notify", "")); err != nil {
		t.Fatalf("first: %v", err)
	}
	other := request(t, "notify", "")
	_, err := registerOnly(t, f, other)
	wantRefused(t, err, program.RefusedNameTaken)
	time.Sleep(100 * time.Millisecond)
	if _, err := f.register(t, other); err != nil {
		t.Fatalf("a new key once the pending one expired: %v", err)
	}
	if f.record(t, "notify").PublicKey != other.PublicKey {
		t.Fatal("the name did not pass to the new key")
	}
}

func TestPossession_PendingWithADifferentManifestIsRefused(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	if _, err := registerOnly(t, f, req); err != nil {
		t.Fatalf("Register: %v", err)
	}
	changed := req
	changed.Manifest = cloneManifest(req.Manifest)
	changed.Manifest.TaskTypes = append(changed.Manifest.TaskTypes, "notify.more")
	_, err := registerOnly(t, f, changed)
	wantRefused(t, err, program.RefusedManifestChanged)
	_, err = confirm(t, f, req.PublicKey, changed)
	wantRefused(t, err, program.RefusedManifestChanged)
}
