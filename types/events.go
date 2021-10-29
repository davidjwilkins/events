package types

import (
	"encoding/json"
	"github.com/davidjwilkins/events/statuses"
	"github.com/google/uuid"
)

type Subject string

const (
	UserCreatedV1 Subject = "users.user:created:v1"
	UserUpdatedV1 Subject = "users.user:updateds:v1"
	UserForgotPasswordV1 Subject = "users.user:forgot-password:v1"
)



type Event struct {
	Subject Subject `json:"subject"`
	Data json.RawMessage `json:"data"`
}

type IncomingEvent struct {
	Event
	ID uuid.UUID `json:"id"`
	Sequence uint64 `json:"sequence"`
	Status statuses.Status `json:"status"`
	Attempts int64 `json:"attempts"`
}

type UserCreatedEventV1 struct {
	ID string `json:"id"`
	Email *string `json:"email"`
	Version int `json:"version"`
}

type UserUpdatedEventV1 struct {
	ID string `json:"id"`
	Email *string `json:"email"`
	Version int `json:"version"`
}

type UserForgotPasswordEventV1 struct {
	Email *string `json:"email"`
	RecoveryToken string `json:"recoveryToken"`
}
