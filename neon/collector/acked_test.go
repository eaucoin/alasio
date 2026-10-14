package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Traces of one span, named name.
func traces(name string) ptrace.Traces {
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName(name)
	return td
}

// An export the kafka exporter answers as taken is in the topic: a broker that appends a
// record, then answers its produce with an error once the export's context is done, makes
// the export fail, and the next export, with the record's sequence, is answered as a
// duplicate of it unless the producer's epoch is bumped first (franz-go #1484).
func TestAnExportAnsweredAsTakenIsInTheTopic(t *testing.T) {
	const topic = "otel.spans"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	factory := kafkaexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	if err := confmap.NewFromStringMap(map[string]any{
		"brokers":          cluster.ListenAddrs(),
		"traces":           map[string]any{"topic": topic, "encoding": "otlp_proto"},
		"producer":         map[string]any{"required_acks": -1},
		"timeout":          "10s",
		"sending_queue":    map[string]any{"enabled": false},
		"retry_on_failure": map[string]any{"enabled": false},
	}).Unmarshal(cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exporter, err := factory.CreateTraces(ctx, exportertest.NewNopSettings(factory.Type()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Start(ctx, componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exporter.Shutdown(context.Background()) }()
	if err := exporter.ConsumeTraces(ctx, traces("first")); err != nil {
		t.Fatalf("first: %v", err)
	}

	// The second export's produce appended, then answered NOT_LEADER_OR_FOLLOWER once its
	// context is done, as a broker that is taking over its partitions again answers.
	raw, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	second, abandon := context.WithCancel(ctx)
	defer abandon()
	var produces atomic.Int32
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		if produces.Add(1) > 1 {
			return nil, nil, false
		}
		produce := request.(*kmsg.ProduceRequest)
		appended := *produce
		cluster.SleepControl(func() {
			if _, err := raw.Request(ctx, &appended); err != nil {
				t.Errorf("appending the second: %v", err)
			}
		})
		abandon()
		response := produce.ResponseKind().(*kmsg.ProduceResponse)
		answered := kmsg.NewProduceResponseTopic()
		answered.Topic = produce.Topics[0].Topic
		answered.TopicID = produce.Topics[0].TopicID
		partition := kmsg.NewProduceResponseTopicPartition()
		partition.ErrorCode = kerr.NotLeaderForPartition.Code
		answered.Partitions = append(answered.Partitions, partition)
		response.Topics = append(response.Topics, answered)
		return response, nil, true
	})
	if err := exporter.ConsumeTraces(second, traces("second")); err == nil {
		t.Fatal("second: answered as taken, where its context was done")
	}
	if err := exporter.ConsumeTraces(ctx, traces("third")); err != nil {
		t.Fatalf("third: %v", err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	var names []string
	unmarshaler := ptrace.ProtoUnmarshaler{}
	for len(names) < 3 {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("the topic holds %v, not the third: %v", names, err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			td, err := unmarshaler.UnmarshalTraces(record.Value)
			if err != nil {
				t.Fatal(err)
			}
			names = append(names, td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
		})
	}
	if names[2] != "third" {
		t.Fatalf("the topic holds %v", names)
	}
}
