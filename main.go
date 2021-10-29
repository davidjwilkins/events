package events

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/davidjwilkins/events/statuses"
	"github.com/davidjwilkins/events/types"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"sync"
)

type SubscriptionHandler struct {
	sync.Mutex
	ctx context.Context
	service string
	db *pgxpool.Pool
	nc *nats.Conn
	js nats.JetStreamContext
	subscriptions map[types.Subject]struct{}
	unsubscribes map[types.Subject]func() error
	close chan <- struct{}
}

func (s *SubscriptionHandler) Close() {
	s.db.Close()
	s.nc.Close()
}

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
}
type Transaction interface {
	Commit() error
	Rollback() error

}

func (s *SubscriptionHandler) Subscribe(topic types.Subject) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.subscriptions[topic]; ok {
		return fmt.Errorf("handler for topic %s already registered", topic)
	}
	s.subscriptions[topic] = struct{}{}
	subscription, err := s.js.QueueSubscribe(string(topic), s.service, func(msg *nats.Msg) {
		data := json.RawMessage(msg.Data)
		event := types.Event{
			Subject: topic,
			Data:    data,
		}
		meta, err := msg.Metadata()
		// TODO: add error logging
		if err != nil {
			_ = msg.Nak()
			return
		}
		sequenceID := meta.Sequence.Stream
		eventID := msg.Header.Get("Nats-Msg-Id")
		eventUUID, err := uuid.Parse(eventID)
		if err != nil {
			_ = msg.Nak()
			return
		}

		dbEvent := types.IncomingEvent{
			Event:    event,
			ID:       eventUUID,
			Sequence: sequenceID,
			Status:   statuses.Pending,
			Attempts: 0,
		}
		_, err = s.db.Exec(
			s.ctx,
			"INSERT INTO incomingEvents (id, subject, data, sequence, status, attempts) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
			dbEvent.ID,
			dbEvent.Subject,
			dbEvent.Data,
			dbEvent.Sequence,
			dbEvent.Attempts,
		)
		if err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	}, nats.Durable(s.service), nats.AckExplicit())
	s.unsubscribes[topic] = subscription.Unsubscribe
	return err
}

func (s *SubscriptionHandler) Unsubscribe(topic types.Subject) error {
	if unsub, ok := s.unsubscribes[topic]; ok {
		err := unsub()
		if err != nil {
			return fmt.Errorf("could not unsubscribe from %s: %w", topic, err)
		}
		delete(s.unsubscribes, topic)
		delete(s.subscriptions, topic)
	}
	return fmt.Errorf("cannot unsubscribe from non-existant subscription %s", topic)
}

func NewSubscriptionHandler(serviceName string, postgresUrl string, natsUrl string) (handler *SubscriptionHandler, err error) {
	err = godotenv.Load(".env")
	if err != nil {
		return
	}
	handler = &SubscriptionHandler{}
	handler.ctx = context.Background()
	handler.service = serviceName
	handler.db, err = pgxpool.Connect(handler.ctx, postgresUrl)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			handler.db.Close()
		}
	}()
	handler.nc, err = nats.Connect(natsUrl)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			handler.nc.Close()
		}
	}()
	handler.js, err = handler.nc.JetStream()
	return
}

func (s *SubscriptionHandler) OnNextEvent(ctx context.Context, fn func (ev types.IncomingEvent, tx pgx.Tx) error) error {
	event := types.IncomingEvent{}
	return s.db.BeginFunc(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `SELECT FOR UPDATE SKIP LOCKED id, subject, data, sequence, status, attempts FROM incomingEvents WHERE status = 'pending' ORDER BY sequence ASC LIMIT 1`)
		err := row.Scan(&event.ID, &event.Subject, &event.Data, &event.Sequence, &event.Status, &event.Attempts)
		if err != nil {
			return err
		}
		err = fn(event, tx)
		event.Attempts++
		if err != nil {
			event.Status = statuses.Error
		} else {
			event.Status = statuses.Processed
		}
		_, err = tx.Exec(ctx, `UPDATE incomingEvents SET status = $1, attempts = $2 WHERE id = $3`, event.Status, event.Attempts, event.ID)
		if err != nil {
			return err
		}
		return nil
	})
}