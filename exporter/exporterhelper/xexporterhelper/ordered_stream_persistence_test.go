// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package xexporterhelper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/hosttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/storagetest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestNewLogsRequestsPersistsRecoveryTailAndReplaysItBeforeQueuedWork(t *testing.T) {
	storageID := component.MustNewIDWithName("file_storage", "ordered")
	storageExtension := storagetest.NewMockStorageExtension(nil)
	host := hosttest.NewHost(map[component.ID]component.Component{storageID: storageExtension})
	queueCfg := exporterhelper.NewDefaultQueueConfig()
	queueCfg.NumConsumers = 1
	queueCfg.Batch = configoptional.Optional[exporterhelper.BatchConfig]{}
	queueCfg.StorageID = &storageID

	type dispatchedItem struct {
		dispatch   Dispatch[plog.Logs]
		completion Completion
	}
	dispatched := make(chan dispatchedItem, 4)
	first, err := NewLogsRequests(
		context.Background(), exportertest.NewNopSettings(exportertest.NopType),
		func(_ context.Context, ld plog.Logs) ([]Descriptor[plog.Logs], error) {
			return []Descriptor[plog.Logs]{{Request: ld, PartitionKey: "persisted-channel", Position: PositionContinue}}, nil
		},
		func(_ context.Context, dispatch Dispatch[plog.Logs], completion Completion) error {
			dispatched <- dispatchedItem{dispatch: dispatch, completion: completion}
			completion.Release()
			return nil
		}, orderedStreamTestSettings(),
		WithQueueBatch(configoptional.Some(queueCfg), NewLogsQueueBatchSettings()),
	)
	require.NoError(t, err)
	require.NoError(t, first.Start(context.Background(), host))
	require.NoError(t, first.ConsumeLogs(context.Background(), orderedStreamTestLogsWithBody("acknowledged-tail")))
	select {
	case item := <-dispatched:
		require.False(t, item.dispatch.Recovery)
		item.completion.Succeed()
	case <-time.After(time.Second):
		t.Fatal("first persistent group was not dispatched")
	}
	require.NoError(t, first.ConsumeLogs(context.Background(), orderedStreamTestLogsWithBody("unresolved-successor")))
	select {
	case item := <-dispatched:
		require.False(t, item.dispatch.Recovery)
	case <-time.After(time.Second):
		t.Fatal("successor queue item was not dispatched")
	}
	require.NoError(t, first.Shutdown(context.Background()))

	replayed := make(chan dispatchedItem, 4)
	second, err := NewLogsRequests(
		context.Background(), exportertest.NewNopSettings(exportertest.NopType),
		func(_ context.Context, ld plog.Logs) ([]Descriptor[plog.Logs], error) {
			return []Descriptor[plog.Logs]{{Request: ld, PartitionKey: "persisted-channel", Position: PositionContinue}}, nil
		}, func(_ context.Context, dispatch Dispatch[plog.Logs], completion Completion) error {
			replayed <- dispatchedItem{dispatch: dispatch, completion: completion}
			completion.Release()
			return nil
		}, orderedStreamTestSettings(),
		WithQueueBatch(configoptional.Some(queueCfg), NewLogsQueueBatchSettings()),
	)
	require.NoError(t, err)
	require.NoError(t, second.Start(context.Background(), host))
	select {
	case tail := <-replayed:
		require.Equal(t, "persisted-channel", tail.dispatch.PartitionKey)
		require.Equal(t, PositionContinue, tail.dispatch.Position)
		require.True(t, tail.dispatch.Recovery, "the acknowledged open-stream tail must be sent first with RECOVER_EVENT")
		require.Equal(t, "acknowledged-tail", orderedStreamTestBody(tail.dispatch.Request))
		tail.completion.Succeed()
	case <-time.After(time.Second):
		t.Fatal("persisted recovery tail was not restored after restart")
	}
	select {
	case successor := <-replayed:
		require.Equal(t, "persisted-channel", successor.dispatch.PartitionKey)
		require.False(t, successor.dispatch.Recovery, "unresolved queue work replays as an ordinary fragment")
		require.Equal(t, "unresolved-successor", orderedStreamTestBody(successor.dispatch.Request))
		successor.completion.Succeed()
	case <-time.After(time.Second):
		t.Fatal("persisted unresolved successor was not replayed after its recovery tail")
	}
	require.NoError(t, second.Shutdown(context.Background()))
}

func TestNewLogsRequestsPersistentMixedGroupSkipsRetiredPrefix(t *testing.T) {
	storageID := component.MustNewIDWithName("file_storage", "mixed")
	host := hosttest.NewHost(map[component.ID]component.Component{storageID: storagetest.NewMockStorageExtension(nil)})
	q := exporterhelper.NewDefaultQueueConfig()
	q.NumConsumers = 1
	q.Batch = configoptional.Optional[exporterhelper.BatchConfig]{}
	q.StorageID = &storageID
	limits := orderedStreamTestSettings()
	limits.MaxConcurrentWrites = 2
	build := func(out chan orderedLogsTestResult) exporter.Logs {
		exp, err := NewLogsRequests(context.Background(), exportertest.NewNopSettings(exportertest.NopType),
			func(_ context.Context, _ plog.Logs) ([]Descriptor[plog.Logs], error) {
				var descriptors []Descriptor[plog.Logs]
				for _, body := range []string{"A", "B", "C"} {
					key := "p"
					if body == "C" {
						key = "q"
					}
					descriptors = append(descriptors, Descriptor[plog.Logs]{Request: orderedStreamTestLogsWithBody(body), PartitionKey: key})
				}
				return descriptors, nil
			}, func(_ context.Context, d Dispatch[plog.Logs], done Completion) error {
				out <- orderedLogsTestResult{d, done}
				return nil
			},
			limits, WithQueueBatch(configoptional.Some(q), NewLogsQueueBatchSettings()))
		require.NoError(t, err)
		require.NoError(t, exp.Start(context.Background(), host))
		return exp
	}
	dispatched := make(chan orderedLogsTestResult, 8)
	first := build(dispatched)
	require.NoError(t, first.ConsumeLogs(context.Background(), orderedStreamTestLogs()))
	a, c := receiveOrderedLogsResult(t, dispatched), receiveOrderedLogsResult(t, dispatched)
	if a.dispatch.PartitionKey == "q" {
		a, c = c, a
	}
	a.completion.Release()
	b := receiveOrderedLogsResult(t, dispatched)
	a.completion.Succeed()
	b.completion.Succeed()
	require.NoError(t, first.Shutdown(context.Background()))
	replayed := make(chan orderedLogsTestResult, 8)
	second := build(replayed)
	t.Cleanup(func() { require.NoError(t, second.Shutdown(context.Background())) })
	tail, pending := receiveOrderedLogsResult(t, replayed), receiveOrderedLogsResult(t, replayed)
	if tail.dispatch.PartitionKey == "q" {
		tail, pending = pending, tail
	}
	require.True(t, tail.dispatch.Recovery)
	require.Equal(t, "B", orderedStreamTestBody(tail.dispatch.Request))
	tail.completion.Release()
	select {
	case d := <-replayed:
		t.Fatalf("retired prefix rewound the stream: %s", orderedStreamTestBody(d.dispatch.Request))
	case <-time.After(20 * time.Millisecond):
	}
	tail.completion.Succeed()
	pending.completion.Succeed()
	_ = c
}
