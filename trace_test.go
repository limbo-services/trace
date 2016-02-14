package trace

import (
	"errors"
	"testing"
)

func TestPanicQuard(t *testing.T) {
	tracer := NewTracer(nil)

	func() {
		span := tracer.Trace("Test", WithPanicGuard)
		defer span.Close()

		span.Log("Hello World")
		span.Logf("Hello %s", "tracer")
		span.Error(errors.New("test error"))
		span.Errorf("som other error")
		span.Fatal("fatal")
	}()
}
