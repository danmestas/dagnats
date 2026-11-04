// console/csrf_race_test.go
// Tests that the CSRF secret is safe to load while tokens are minted:
// two servers in one process (the cluster tests) each load it at
// startup while the other may already serve the console. Methodology:
// loads and token checks race from several goroutines; run under -race,
// which reports the unsynchronized access this guards against. Bounded
// iteration count.
package console

import (
	"sync"
	"testing"
)

func TestCSRFSecretLoadIsSafeAlongsideTokens(t *testing.T) {
	if _, _, err := LoadCSRFSecret("first"); err != nil {
		t.Fatalf("LoadCSRFSecret: %v", err)
	}
	actor := Actor{User: "ops"}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, _, err := LoadCSRFSecret("first"); err != nil {
					t.Errorf("LoadCSRFSecret: %v", err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = VerifyCSRFToken(actor, CSRFTokenForActor(actor))
			}
		}()
	}
	wg.Wait()
	// Positive: a token minted under the secret verifies.
	if !VerifyCSRFToken(actor, CSRFTokenForActor(actor)) {
		t.Fatal("token for ops does not verify")
	}
	// Negative: a token for another actor does not.
	if VerifyCSRFToken(actor, CSRFTokenForActor(Actor{User: "intruder"})) {
		t.Fatal("another actor's token verified")
	}
}
