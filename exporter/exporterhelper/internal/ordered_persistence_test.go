// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/experr"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
)

type orderedTestCheckpointStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	loadErr  error
	failSave int
}

func (s *orderedTestCheckpointStore) LoadCheckpoint(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, false, s.loadErr
	}
	value, ok := s.data[key]
	return append([]byte(nil), value...), ok, nil
}

func (s *orderedTestCheckpointStore) SaveCheckpoint(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSave > 0 {
		s.failSave--
		return errors.New("temporary checkpoint storage failure")
	}
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	s.data[key] = append([]byte(nil), value...)
	return nil
}

func (s *orderedTestCheckpointStore) SaveCheckpointAndItems(_ context.Context, key string, value []byte, updates []request.QueueItemUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSave > 0 {
		s.failSave--
		return errors.New("temporary checkpoint storage failure")
	}
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	s.data[key] = append([]byte(nil), value...)
	for _, update := range updates {
		s.data[fmt.Sprintf("item-%d", update.Token)] = append([]byte(nil), update.Value...)
	}
	return nil
}

func checkpointTail(t *testing.T, store *orderedTestCheckpointStore, g *orderedLogsGroup) {
	t.Helper()
	encoded, err := (orderedLogsEncoding{}).Marshal(context.Background(), g)
	require.NoError(t, err)
	store.data[orderedLogsCheckpointKey] = encoded
}

func TestOrderedCoordinatorKeepsQueueItemUntilTailCheckpointIsDurable(t *testing.T) {
	dispatched := make(chan orderedDispatchResult, 1)
	coordinator := newOrderedLogsCoordinator(orderedTestSettings(), func(_ context.Context, dispatch OrderedLogsDispatch, completion OrderedLogsCompletion) error {
		dispatched <- orderedDispatchResult{dispatch: dispatch, completion: completion}
		return nil
	}, configretry.BackOffConfig{}, 0, 1, nil)
	t.Cleanup(coordinator.shutdown)
	store := &orderedTestCheckpointStore{failSave: 1}
	group := orderedTestGroup("channel-durable-tail", OrderedPositionContinue)
	group.SetQueueCheckpointStore(store)
	done := make(chan error, 1)
	require.True(t, group.SetQueueCompletion(func(err error) { done <- err }))
	require.NoError(t, coordinator.add(context.Background(), group))
	dispatch := receiveOrderedDispatch(t, dispatched)
	dispatch.completion.Release()
	dispatch.completion.Succeed()
	select {
	case err := <-done:
		t.Fatalf("queue item retired before its recovery-tail checkpoint was durable: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("queue item did not retire after checkpoint storage recovered")
	}
	store.mu.Lock()
	checkpoint := store.data[orderedLogsCheckpointKey]
	store.mu.Unlock()
	require.NotEmpty(t, checkpoint)
}

func TestOrderedCoordinatorDoesNotReplayCheckpointedQueueItemTwice(t *testing.T) {
	dispatched := make(chan orderedDispatchResult, 2)
	store := &orderedTestCheckpointStore{data: make(map[string][]byte)}
	tail := orderedTestGroup("channel-duplicate-tail", OrderedPositionContinue)
	encoded, err := (orderedLogsEncoding{}).Marshal(context.Background(), tail)
	require.NoError(t, err)
	store.data[orderedLogsCheckpointKey] = encoded

	coordinator := newOrderedLogsCoordinator(orderedTestSettings(), func(_ context.Context, dispatch OrderedLogsDispatch, completion OrderedLogsCompletion) error {
		dispatched <- orderedDispatchResult{dispatch: dispatch, completion: completion}
		return nil
	}, configretry.BackOffConfig{}, 0, 1, nil)
	t.Cleanup(coordinator.shutdown)
	queuedDuplicate := orderedTestGroup("channel-duplicate-tail", OrderedPositionContinue)
	queuedDuplicate.children[0].QueueID = tail.children[0].QueueID
	queuedDuplicateDone := make(chan error, 1)
	queuedDuplicate.SetQueueCheckpointStore(store)
	require.True(t, queuedDuplicate.SetQueueCompletion(func(err error) { queuedDuplicateDone <- err }))
	require.NoError(t, coordinator.add(context.Background(), queuedDuplicate))

	recovery := receiveOrderedDispatch(t, dispatched)
	require.True(t, recovery.dispatch.Recovery)
	recovery.completion.Release()
	recovery.completion.Succeed()
	require.NoError(t, <-queuedDuplicateDone)
	select {
	case duplicate := <-dispatched:
		t.Fatalf("checkpointed tail was also dispatched as ordinary queue work: %+v", duplicate.dispatch)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestOrderedCoordinatorClearsPersistentTailAfterEnd(t *testing.T) {
	dispatched := make(chan orderedDispatchResult, 2)
	store := &orderedTestCheckpointStore{data: make(map[string][]byte)}
	coordinator := newOrderedLogsCoordinator(orderedTestSettings(), func(_ context.Context, dispatch OrderedLogsDispatch, completion OrderedLogsCompletion) error {
		dispatched <- orderedDispatchResult{dispatch: dispatch, completion: completion}
		return nil
	}, configretry.BackOffConfig{}, 0, 1, nil)
	t.Cleanup(coordinator.shutdown)

	continuation := orderedTestGroup("channel-end", OrderedPositionContinue)
	continuation.SetQueueCheckpointStore(store)
	continuationDone := make(chan error, 1)
	require.True(t, continuation.SetQueueCompletion(func(err error) { continuationDone <- err }))
	require.NoError(t, coordinator.add(context.Background(), continuation))
	continued := receiveOrderedDispatch(t, dispatched)
	continued.completion.Release()
	continued.completion.Succeed()
	require.NoError(t, <-continuationDone)
	store.mu.Lock()
	checkpointWithTail := append([]byte(nil), store.data[orderedLogsCheckpointKey]...)
	store.mu.Unlock()
	_, decoded, err := (orderedLogsEncoding{}).Unmarshal(checkpointWithTail)
	require.NoError(t, err)
	require.Len(t, decoded.(*orderedLogsGroup).children, 1)

	end := orderedTestGroup("channel-end", OrderedPositionEnd)
	end.SetQueueCheckpointStore(store)
	endDone := make(chan error, 1)
	require.True(t, end.SetQueueCompletion(func(err error) { endDone <- err }))
	require.NoError(t, coordinator.add(context.Background(), end))
	ended := receiveOrderedDispatch(t, dispatched)
	ended.completion.Release()
	ended.completion.Succeed()
	require.NoError(t, <-endDone)
	store.mu.Lock()
	checkpointWithoutTail := append([]byte(nil), store.data[orderedLogsCheckpointKey]...)
	store.mu.Unlock()
	_, decoded, err = (orderedLogsEncoding{}).Unmarshal(checkpointWithoutTail)
	require.NoError(t, err)
	require.Empty(t, decoded.(*orderedLogsGroup).children)
}

func TestOrderedCoordinatorShutdownCancelsRestoredRecoveryPrelude(t *testing.T) {
	store := &orderedTestCheckpointStore{data: make(map[string][]byte)}
	tail := orderedTestGroup("channel-shutdown-prelude", OrderedPositionContinue)
	encoded, err := (orderedLogsEncoding{}).Marshal(context.Background(), tail)
	require.NoError(t, err)
	store.data[orderedLogsCheckpointKey] = encoded
	started := make(chan struct{})
	canceled := make(chan struct{})
	coordinator := newOrderedLogsCoordinator(orderedTestSettings(), func(ctx context.Context, dispatch OrderedLogsDispatch, _ OrderedLogsCompletion) error {
		if dispatch.Recovery {
			close(started)
			<-ctx.Done()
			close(canceled)
		}
		return ctx.Err()
	}, configretry.BackOffConfig{}, 0, 1, nil)
	queued := orderedTestGroup("channel-shutdown-prelude", OrderedPositionContinue)
	queued.SetQueueCheckpointStore(store)
	done := make(chan error, 1)
	require.True(t, queued.SetQueueCompletion(func(err error) { done <- err }))
	require.NoError(t, coordinator.add(context.Background(), queued))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("restored recovery prelude was not dispatched")
	}
	coordinator.shutdown()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel the recovery prelude context")
	}
	require.True(t, experr.IsShutdownErr(<-done))
}

func TestOrderedCoordinatorPreservesQueueItemWhenCheckpointLoadFails(t *testing.T) {
	coordinator := newOrderedLogsCoordinator(orderedTestSettings(), func(context.Context, OrderedLogsDispatch, OrderedLogsCompletion) error {
		t.Fatal("request must not dispatch without restored checkpoint state")
		return nil
	}, configretry.BackOffConfig{}, 0, 1, nil)
	t.Cleanup(coordinator.shutdown)
	group := orderedTestGroup("channel-checkpoint-read", OrderedPositionContinue)
	group.SetQueueCheckpointStore(&orderedTestCheckpointStore{loadErr: errors.New("checkpoint storage unavailable")})
	done := make(chan error, 1)
	require.True(t, group.SetQueueCompletion(func(err error) { done <- err }))
	err := coordinator.add(context.Background(), group)
	require.Error(t, err)
	require.True(t, experr.IsShutdownErr(err), "persistent queue must retain the request for a later recovery attempt")
	require.Zero(t, coordinator.staged)
}

func TestOrderedLogsGroupEncodingRoundTrip(t *testing.T) {
	group := orderedTestGroup("channel-with-binary-\x00-key", OrderedPositionEnd)
	encoded, err := (orderedLogsEncoding{}).Marshal(context.Background(), group)
	require.NoError(t, err)
	_, decodedRequest, err := (orderedLogsEncoding{}).Unmarshal(encoded)
	require.NoError(t, err)
	decoded := decodedRequest.(*orderedLogsGroup)
	require.Len(t, decoded.children, 1)
	require.Equal(t, group.children[0].PartitionKey, decoded.children[0].PartitionKey)
	require.Equal(t, OrderedPositionEnd, decoded.children[0].Position)
	require.Equal(t, group.children[0].QueueID, decoded.children[0].QueueID)
	require.Equal(t, 1, decoded.ItemsCount())
	encoded[4] = 2
	_, _, err = (orderedLogsEncoding{}).Unmarshal(encoded)
	require.ErrorContains(t, err, "invalid group header", "unreleased prototype formats are not an upstream compatibility contract")
}

func TestOrderedRecoveryOverlapAllowsSuccessorBeforeTailACK(t *testing.T) {
	for _, workers := range []int{1, 32} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			store := &orderedTestCheckpointStore{data: make(map[string][]byte)}
			a := orderedTestGroup("p", OrderedPositionContinue)
			checkpointTail(t, store, a)
			c, dispatched := regressionCoordinator(t, workers)
			a.SetQueueCheckpointStore(store)
			done := make(chan error, 1)
			a.SetQueueCompletion(func(err error) { done <- err })
			require.NoError(t, c.add(context.Background(), a))
			recovery := receiveOrderedDispatch(t, dispatched)
			b := orderedTestGroup("p", OrderedPositionEnd)
			b.SetQueueCompletion(func(error) {})
			require.NoError(t, c.add(context.Background(), b))
			recovery.completion.Release()
			successor := receiveOrderedDispatch(t, dispatched)
			require.Equal(t, b.children[0].QueueID, successor.dispatch.QueueID)
			successor.completion.Succeed()
			recovery.completion.Succeed()
			require.NoError(t, <-done)
			select {
			case d := <-dispatched:
				t.Fatalf("unexpected duplicate: %+v", d.dispatch)
			default:
			}
		})
	}
}

func TestOrderedRecoveryACKDoesNotReleaseSuccessorWrite(t *testing.T) {
	store := &orderedTestCheckpointStore{data: make(map[string][]byte)}
	checkpointTail(t, store, orderedTestGroup("p", OrderedPositionContinue))
	c, dispatched := regressionCoordinator(t, 1)
	b := orderedTestGroup("p", OrderedPositionContinue)
	b.SetQueueCheckpointStore(store)
	b.SetQueueCompletion(func(error) {})
	require.NoError(t, c.add(context.Background(), b))
	recovery := receiveOrderedDispatch(t, dispatched)
	recovery.completion.Release()
	active := receiveOrderedDispatch(t, dispatched)
	next := orderedTestGroup("p", OrderedPositionEnd)
	next.SetQueueCompletion(func(error) {})
	require.NoError(t, c.add(context.Background(), next))
	recovery.completion.Succeed()
	select {
	case d := <-dispatched:
		t.Fatalf("old ACK released successor: %+v", d.dispatch)
	case <-time.After(20 * time.Millisecond):
	}
	c.mu.Lock()
	require.Equal(t, 1, c.activeWrites)
	c.mu.Unlock()
	active.completion.Release()
	last := receiveOrderedDispatch(t, dispatched)
	require.Equal(t, next.children[0].QueueID, last.dispatch.QueueID)
	active.completion.Succeed()
	last.completion.Succeed()
}

func TestOrderedPersistentGroupRetiresChildrenAtomicallyWithTail(t *testing.T) {
	store := &orderedTestCheckpointStore{data: make(map[string][]byte)}
	c, dispatched := regressionCoordinator(t, 2)
	a, b, q := orderedTestGroup("p", OrderedPositionContinue), orderedTestGroup("p", OrderedPositionContinue), orderedTestGroup("q", OrderedPositionContinue)
	group := &orderedLogsGroup{children: []OrderedLogsDescriptor{a.children[0], b.children[0], q.children[0]}, items: 3, bytes: a.bytes + b.bytes + q.bytes}
	group.SetQueueItemToken(7)
	group.SetQueueCheckpointStore(store)
	group.SetQueueCompletion(func(error) {})
	require.NoError(t, c.add(context.Background(), group))
	first, other := receiveOrderedDispatch(t, dispatched), receiveOrderedDispatch(t, dispatched)
	if first.dispatch.PartitionKey == "q" {
		first, other = other, first
	}
	first.completion.Release()
	second := receiveOrderedDispatch(t, dispatched)
	first.completion.Succeed()
	second.completion.Succeed()
	store.mu.Lock()
	body := append([]byte(nil), store.data["item-7"]...)
	store.mu.Unlock()
	_, decoded, err := (orderedLogsEncoding{}).Unmarshal(body)
	require.NoError(t, err)
	restored := decoded.(*orderedLogsGroup)
	require.True(t, restored.retired[a.children[0].QueueID])
	require.True(t, restored.retired[b.children[0].QueueID])
	require.False(t, restored.retired[q.children[0].QueueID])
	c.shutdown()
	restarted, replay := regressionCoordinator(t, 2)
	restored.SetQueueItemToken(7)
	restored.SetQueueCheckpointStore(store)
	restored.SetQueueCompletion(func(error) {})
	require.NoError(t, restarted.add(context.Background(), restored))
	prelude, unresolved := receiveOrderedDispatch(t, replay), receiveOrderedDispatch(t, replay)
	if prelude.dispatch.PartitionKey == "q" {
		prelude, unresolved = unresolved, prelude
	}
	require.True(t, prelude.dispatch.Recovery)
	require.Equal(t, b.children[0].QueueID, prelude.dispatch.QueueID)
	prelude.completion.Release()
	successor := orderedTestGroup("p", OrderedPositionEnd)
	successor.SetQueueCompletion(func(error) {})
	require.NoError(t, restarted.add(context.Background(), successor))
	next := receiveOrderedDispatch(t, replay)
	require.Equal(t, successor.children[0].QueueID, next.dispatch.QueueID, "retired A must never follow recovery B")
	prelude.completion.Succeed()
	unresolved.completion.Succeed()
	next.completion.Succeed()
	_ = other
}

func TestOrderedRetiredEnvelopeDoesNotConsumePartitionCapacity(t *testing.T) {
	store := &orderedTestCheckpointStore{data: make(map[string][]byte)}
	checkpointTail(t, store, orderedTestGroup("q", OrderedPositionContinue))
	settings := orderedTestSettings()
	settings.MaxActivePartitions = 1
	c := newOrderedLogsCoordinator(settings, func(context.Context, OrderedLogsDispatch, OrderedLogsCompletion) error { return nil }, configretry.BackOffConfig{}, 0, 1, nil)
	t.Cleanup(c.shutdown)
	group := orderedTestGroup("p", OrderedPositionEnd)
	group.retired = map[[16]byte]bool{group.children[0].QueueID: true}
	group.SetQueueCheckpointStore(store)
	done := make(chan error, 1)
	group.SetQueueCompletion(func(err error) { done <- err })
	added := make(chan error, 1)
	go func() { added <- c.add(context.Background(), group) }()
	select {
	case err := <-added:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("retired envelope waited for a partition slot")
	}
	require.NoError(t, <-done)
}

func TestOrderedRecoveryErrorFencesLaterQueueItems(t *testing.T) {
	store := &orderedTestCheckpointStore{loadErr: errors.New("disk I/O failure")}
	c, dispatched := regressionCoordinator(t, 1)
	a := orderedTestGroup("p", OrderedPositionContinue)
	a.SetQueueCheckpointStore(store)
	require.Error(t, c.add(context.Background(), a))
	store.loadErr = nil
	b := orderedTestGroup("p", OrderedPositionContinue)
	b.SetQueueCheckpointStore(store)
	require.Error(t, c.add(context.Background(), b))
	select {
	case <-dispatched:
		t.Fatal("later item overtook a recovery failure")
	default:
	}
	c.mu.Lock()
	require.Empty(t, c.parts)
	c.mu.Unlock()
}
