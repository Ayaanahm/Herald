package store_test

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/Ayaanahm/Herald/internal/store"
	"github.com/Ayaanahm/Herald/internal/testenv"
)

// The API marks batches of rows published while workers claim and finish
// batches of the same rows. Multi-row UPDATEs that lock rows in different
// orders deadlock, and PostgreSQL aborts one of them. Found under load in
// bench/chaos.sh; every batched update must lock rows in id order.
func TestBatchedUpdatesDoNotDeadlock(t *testing.T) {
	st := testenv.Store(t)
	ctx := context.Background()
	items := make([]store.NewNotification, 1000)
	for i := range items {
		id, _ := uuid.NewV7()
		items[i] = store.NewNotification{ID: id, TenantID: "t", RecipientID: "u", Channels: []string{"push"}, Payload: json.RawMessage(`{}`)}
	}
	if _, err := st.InsertBatch(ctx, items); err != nil {
		t.Fatal(err)
	}
	ids := make([]uuid.UUID, len(items))
	for i := range items {
		ids[i] = items[i].ID
	}
	shuffled := func(r *rand.Rand) []uuid.UUID {
		s := append([]uuid.UUID(nil), ids...)
		r.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
		return s[:300]
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 7))
			for i := 0; i < 15; i++ {
				batch := shuffled(r)
				var err error
				switch i % 3 {
				case 0:
					err = st.MarkPublished(ctx, batch)
				case 1:
					reqs := make([]store.ClaimReq, len(batch))
					for j, id := range batch {
						reqs[j] = store.ClaimReq{ID: id, Fence: int64(1 + g*1000 + i)}
					}
					_, err = st.ClaimMany(ctx, reqs)
				case 2:
					reqs := make([]store.DeliveredReq, len(batch))
					for j, id := range batch {
						reqs[j] = store.DeliveredReq{ID: id, Fence: 0, Channel: "push"}
					}
					_, err = st.MarkDeliveredMany(ctx, reqs)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
