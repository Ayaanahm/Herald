// Package queue defines Herald's Kafka topics and message format.
//
// Topics:
//
//	herald.notifications     new work, keyed by tenant/recipient so one recipient's
//	                         notifications stay in order within a partition
//	herald.retry.<delay>     one topic per retry delay (e.g. 1s, 5s, 15s). Every
//	                         message in a topic waits the same delay, so each
//	                         partition is already sorted by due time and a
//	                         consumer only has to sleep until the head is due.
//	herald.dlq               notifications that failed on every channel, plus
//	                         poison messages that could not be decoded
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// prefix lets tests (and multiple environments on one cluster) use separate topics.
var prefix = "herald"

// SetPrefix changes the topic prefix. Call it once at startup, before any client is created.
func SetPrefix(p string) {
	if p != "" {
		prefix = p
	}
}

func Notifications() string { return prefix + ".notifications" }
func DLQ() string           { return prefix + ".dlq" }

func RetryTopic(d time.Duration) string { return prefix + ".retry." + d.String() }

// Message is one unit of delivery work.
type Message struct {
	ID          uuid.UUID       `json:"id"`
	TenantID    string          `json:"tenant_id"`
	RecipientID string          `json:"recipient_id"`
	Channels    []string        `json:"channels"`
	CollapseKey string          `json:"collapse_key,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	Seq         int64           `json:"seq"`
	Step        int             `json:"step"`                 // must match the row's step, or the message is stale
	ChannelIdx  int             `json:"channel_idx"`          // index into Channels
	Attempt     int             `json:"attempt"`              // attempt number on the current channel
	NotBefore   int64           `json:"not_before,omitempty"` // unix ms; retry messages wait until then
	CreatedAt   int64           `json:"created_at"`           // unix ms, for end-to-end latency
	Errors      []string        `json:"errors,omitempty"`     // history, carried into the DLQ
}

// Key keeps one recipient's notifications on one partition.
func (m *Message) Key() []byte { return []byte(m.TenantID + "/" + m.RecipientID) }

func (m *Message) Record(topic string) (*kgo.Record, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &kgo.Record{Topic: topic, Key: m.Key(), Value: b}, nil
}

func Decode(b []byte) (Message, error) {
	var m Message
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	if m.ID == uuid.Nil || m.TenantID == "" || len(m.Channels) == 0 || m.ChannelIdx >= len(m.Channels) {
		return m, fmt.Errorf("invalid message")
	}
	return m, nil
}

// NewProducer returns an idempotent producer: acks from all in-sync replicas,
// and the broker de-duplicates producer retries.
func NewProducer(brokers []string, extra ...kgo.Opt) (*kgo.Client, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(2 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}, extra...)
	return kgo.NewClient(opts...)
}

// EnsureTopics creates every topic Herald uses, if missing.
func EnsureTopics(ctx context.Context, brokers []string, partitions int, retryDelays []time.Duration) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	topics := []string{Notifications(), DLQ()}
	for _, d := range retryDelays {
		topics = append(topics, RetryTopic(d))
	}
	res, err := adm.CreateTopics(ctx, int32(partitions), -1, nil, topics...)
	if err != nil {
		return err
	}
	for _, r := range res.Sorted() {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w (%s)", r.Topic, r.Err, r.ErrMessage)
		}
	}
	return nil
}
