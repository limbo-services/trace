//+build dev

package gcp

import (
	"errors"
	"log"
	"testing"
	"time"

	"golang.org/x/net/context"

	"limbo.services/trace"
)

func Test(t *testing.T) {
	handler, err := New(&Config{
		Bucket:       "trace-sets",
		ProjectID:    "hypnotic-surge-101511",
		Topic:        "trace-sets",
		Subscription: "trace-set-worker",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = handler.HandleTrace(nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Minute)
	defer cancel()
	tracer := trace.NewTracer(handler)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			// time.Sleep(50 * time.Millisecond)
			mockTraces(tracer)
		}
	}()

	beg := time.Now()
	cnt := 0

	traces := handler.Pull(ctx)
	for trace := range traces {
		cnt++
		_ = trace
		// log.Printf("trace: %v", trace)
	}

	delta := time.Since(beg)
	log.Printf("%d traces in %s", cnt, delta)
}

func mockTraces(tracer *trace.Tracer) {
	span := tracer.Trace("Test", trace.WithPanicGuard)
	defer span.Close()

	span.Log("Hello World")
	span.Logf("Hello %s", "tracer")
	span.Error(errors.New("test error"))
	span.Errorf("som other error")
	span.Fatal("fatal")
}
