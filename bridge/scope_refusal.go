// bridge/scope_refusal.go
// The 403 a valid worker token gets for a task type or worker group outside
// its scope (#1026). The sentence stays in "error", and the token's scope
// and what was asked for are fields beside it, so a caller can tell the
// token's holder where the token belongs without parsing English. Only a
// token that authenticated reaches this: an unknown or revoked one gets the
// bare 401 and learns nothing about any token.
package bridge

import (
	"net/http"

	"github.com/danmestas/dagnats/internal/workertoken"
)

// Codes of a scope refusal.
const (
	codeTaskTypeNotAllowed    = "task_type_not_allowed"
	codeWorkerGroupNotAllowed = "worker_group_not_allowed"
)

// scopeRefusal is the JSON body of a scope 403.
type scopeRefusal struct {
	Message           string   `json:"error"`
	Code              string   `json:"code"`
	Requested         string   `json:"requested"`
	TokenTaskTypes    []string `json:"token_task_types"`
	TokenWorkerGroups []string `json:"token_worker_groups"`
}

func (r *scopeRefusal) Error() string { return r.Message }

// newScopeRefusal builds a refusal of requested under claims with message.
// The scope lists are never null, so a caller can always iterate them.
func newScopeRefusal(claims workertoken.Claims, code, requested, message string) *scopeRefusal {
	if claims.Admin {
		panic("newScopeRefusal: an admin token is never refused for scope")
	}
	if code != codeTaskTypeNotAllowed && code != codeWorkerGroupNotAllowed {
		panic("newScopeRefusal: unknown code " + code)
	}
	return &scopeRefusal{
		Message: message, Code: code, Requested: requested,
		TokenTaskTypes:    append([]string{}, claims.TaskTypePrefixes...),
		TokenWorkerGroups: append([]string{}, claims.WorkerGroups...),
	}
}

// taskTypeRefusal is the refusal of taskType, which claims does not allow.
func taskTypeRefusal(claims workertoken.Claims, taskType string) *scopeRefusal {
	return newScopeRefusal(claims, codeTaskTypeNotAllowed, taskType,
		claims.ExplainTaskTypeRefusal(taskType))
}

// workerGroupRefusal is the refusal of group, which claims does not allow.
func workerGroupRefusal(claims workertoken.Claims, group string) *scopeRefusal {
	return newScopeRefusal(claims, codeWorkerGroupNotAllowed, group,
		claims.ExplainWorkerGroupRefusal(group))
}

// write answers the refusal as a JSON 403.
func (r *scopeRefusal) write(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, r)
}
