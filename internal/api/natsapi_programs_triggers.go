package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// programTriggerSource labels the triggers a program created, so create
// and delete touch only those and never an operator's trigger that
// happens to sit under the program's prefix (#957).
func programTriggerSource(prefix string) string {
	if prefix == "" {
		panic("programTriggerSource: prefix must not be empty")
	}
	return "program:" + strings.TrimSuffix(prefix, ".")
}

// confineTrigger refuses anything but a cron trigger for the program's
// own workflow under the program's own ID prefix.
func confineTrigger(def trigger.TriggerDef, prefix string) error {
	if prefix == "" {
		panic("confineTrigger: prefix must not be empty")
	}
	if !underPrefix(def.ID, prefix) {
		return fmt.Errorf("trigger id %q is not under %q", def.ID, prefix)
	}
	if !underPrefix(def.WorkflowID, prefix) {
		return fmt.Errorf("workflow %q is not under %q", def.WorkflowID, prefix)
	}
	other := def.Subject != nil || def.Webhook != nil || def.HTTP != nil ||
		def.External != nil || def.RunTerminal != nil || def.Debounce != nil
	if def.Cron == nil || other {
		return errors.New("a program may register cron triggers only")
	}
	return nil
}

func (n *NATSAPI) handleProgramTriggerCreate(req micro.Request) {
	if req == nil || n.svc == nil {
		panic("handleProgramTriggerCreate: req and svc must not be nil")
	}
	prefix, err := programPrefix(req.Subject())
	if err != nil {
		n.replyErr(req, err)
		return
	}
	var def trigger.TriggerDef
	if err := json.Unmarshal(req.Data(), &def); err != nil {
		n.replyErr(req, err)
		return
	}
	if err := confineTrigger(def, prefix); err != nil {
		n.replyErr(req, err)
		return
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	source := programTriggerSource(prefix)
	existing, found, err := n.svc.getTrigger(ctx, def.ID)
	if err != nil {
		n.replyErr(req, err)
		return
	}
	if found && existing.Source != source {
		n.replyErr(req, fmt.Errorf("trigger %q was not created by this program", def.ID))
		return
	}
	def.Source = source
	if err := n.svc.CreateTrigger(ctx, def); err != nil {
		n.replyErr(req, err)
		return
	}
	n.reply(req, map[string]string{"status": "created", "id": def.ID})
}

func (n *NATSAPI) handleProgramTriggerDelete(req micro.Request) {
	if req == nil || n.svc == nil {
		panic("handleProgramTriggerDelete: req and svc must not be nil")
	}
	prefix, err := programPrefix(req.Subject())
	if err != nil {
		n.replyErr(req, err)
		return
	}
	id := string(req.Data())
	if id == "" {
		n.replyErr(req, errors.New("trigger id is required"))
		return
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	// One error for a foreign, operator-owned or missing trigger, so a
	// program cannot probe what exists outside its own.
	notFound := errors.New("trigger not found")
	if !underPrefix(id, prefix) {
		n.replyErr(req, notFound)
		return
	}
	existing, found, err := n.svc.getTrigger(ctx, id)
	if err != nil {
		n.replyErr(req, err)
		return
	}
	if !found || existing.Source != programTriggerSource(prefix) {
		n.replyErr(req, notFound)
		return
	}
	if err := n.svc.DeleteTrigger(ctx, id); err != nil {
		n.replyErr(req, err)
		return
	}
	n.reply(req, map[string]string{"status": "deleted", "id": id})
}
