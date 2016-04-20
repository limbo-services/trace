package gcp // import "limbo.services/trace/gcp"

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/pubsub"
	"google.golang.org/cloud/storage"

	"limbo.services/trace"
)

type Config struct {
	ProjectID    string
	Topic        string
	Subscription string
	Bucket       string
}

func New(config *Config) (*Hander, error) {
	ctx := context.Background()

	ts, err := google.DefaultTokenSource(ctx,
		pubsub.ScopeCloudPlatform,
		pubsub.ScopePubSub,
		storage.ScopeReadWrite)
	if err != nil {
		return nil, errors.Trace(err)
	}

	storageClient, err := storage.NewClient(ctx,
		cloud.WithTokenSource(ts))
	if err != nil {
		return nil, errors.Trace(err)
	}

	handler := &Hander{
		pubsub:       cloud.WithContext(ctx, config.ProjectID, oauth2.NewClient(ctx, ts)),
		topic:        config.Topic,
		subscription: config.Subscription,
		bucket:       storageClient.Bucket(config.Bucket),
	}

	return handler, nil
}

type Hander struct {
	pubsub       context.Context
	topic        string
	subscription string
	bucket       *storage.BucketHandle

	runOnce       sync.Once
	mtx           sync.Mutex
	tmpFile       *os.File
	tmpCompresser *gzip.Writer
	tmpBuffer     *bufio.Writer
}

func (h *Hander) Pull(ctx context.Context) <-chan *trace.Trace {
	out := make(chan *trace.Trace)
	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			h.pull(ctx, out)
		}
	}()
	return out
}
func (h *Hander) pull(ctx context.Context, out chan<- *trace.Trace) {
	subCtx, close := context.WithCancel(h.pubsub)
	defer close()

	go func() {
		select {
		case <-subCtx.Done():
			close()
		case <-ctx.Done():
			close()
		case <-h.pubsub.Done():
			close()
		}
	}()

	messages, err := pubsub.PullWait(subCtx, h.subscription, 10)
	if err != nil {
		log.Printf("trace: pull error: %s", err)
		return
	}

	for _, message := range messages {
		go h.handleMessage(out, message)
	}
}

func (h *Hander) handleMessage(out chan<- *trace.Trace, message *pubsub.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	defer h.messageAck(ctx, message)
	go h.messageAckExtender(ctx, message)

	payload := string(message.Data)
	if !strings.HasPrefix(payload, "trace-set://") {
		log.Printf("invalid message: %q", message.Data)
		return
	}
	log.Printf("PULL: %q", payload)
	payload = strings.TrimPrefix(payload, "trace-set://")

	obj := h.bucket.Object(payload + ".json.gz")
	defer obj.Delete(ctx)

	r, err := obj.NewReader(ctx)
	if err != nil {
		log.Printf("trace: gce: read trace-set error: %s", err)
		return
	}

	defer r.Close()

	cr, err := gzip.NewReader(r)
	if err != nil {
		log.Printf("trace: gce: read trace-set error: %s", err)
		return
	}

	dec := json.NewDecoder(cr)

	for {
		var trace *trace.Trace
		err := dec.Decode(&trace)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("trace: gce: read trace-set error: %s", err)
			return
		}

		out <- trace
	}
}

func (h *Hander) messageAck(ctx context.Context, message *pubsub.Message) {
	defer log.Printf("ACK:  %q", message.Data)
	err := pubsub.Ack(h.pubsub, h.subscription, message.AckID)
	if err != nil {
		log.Printf("trace: gce: ack error: %s", err)
	}
}

func (h *Hander) messageAckExtender(ctx context.Context, message *pubsub.Message) {
	ticker := time.NewTicker(200 * time.Second)
	defer ticker.Stop()

	for {
		err := pubsub.ModifyAckDeadline(h.pubsub, h.subscription, message.AckID, 300*time.Second)
		if err != nil {
			log.Printf("trace: gce: pull ack extend error: %s", err)
		} else {
			log.Printf("EACK: %q", message.Data)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (h *Hander) HandleTrace(trace *trace.Trace) error {
	h.runOnce.Do(h.start)

	if trace == nil {
		return nil
	}

	data, err := json.Marshal(trace)
	if err != nil {
		return err
	}

	h.mtx.Lock()
	defer h.mtx.Unlock()

	if h.tmpFile == nil {
		f, err := ioutil.TempFile("", "trace-set-")
		if err != nil {
			return err
		}

		c := gzip.NewWriter(f)
		b := bufio.NewWriter(c)

		h.tmpFile = f
		h.tmpCompresser = c
		h.tmpBuffer = b
	}

	h.tmpBuffer.Write(data)
	h.tmpBuffer.WriteByte('\n')
	return nil
}

func (h *Hander) start() {
	go h.run()
}

func (h *Hander) run() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for _ = range ticker.C {
		go h.Flush()
	}
}

func (h *Hander) Flush() {
	h.mtx.Lock()
	tmpFile, tmpCompresser, tmpBuffer := h.tmpFile, h.tmpCompresser, h.tmpBuffer
	h.tmpFile, h.tmpCompresser, h.tmpBuffer = nil, nil, nil
	h.mtx.Unlock()

	if tmpFile == nil {
		return
	}

	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	if err := tmpBuffer.Flush(); err != nil {
		log.Printf("trace: buffer flush failed: %s", err)
		return
	}

	if err := tmpCompresser.Close(); err != nil {
		log.Printf("trace: compresser close failed: %s", err)
		return
	}

	var (
		id  string
		err error
	)

	for i := 0; i < 3; i++ {
		if _, err := tmpFile.Seek(0, 0); err != nil {
			log.Printf("trace: tmpfile seek failed: %s", err)
			return
		}

		id, err = h.uploadTraces(tmpFile)
		if err != nil {
			log.Printf("trace: gce upload failed: %s", err)
		}
		break
	}
	if err != nil {
		return
	}

	for i := 0; i < 3; i++ {
		_, err = pubsub.Publish(h.pubsub, h.topic, &pubsub.Message{
			Data: []byte("trace-set://" + id),
		})
		if err != nil {
			log.Printf("trace: gce publish failed: %s", err)
		}
		break
	}
	if err != nil {
		return
	}
}

var errUploadFailed = errors.New("upload failed")

func (h *Hander) uploadTraces(traces *os.File) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := uuid.New()
	obj := h.bucket.Object(id + ".json.gz")
	w := obj.NewWriter(ctx)
	defer w.CloseWithError(errUploadFailed)

	_, err := io.Copy(w, traces)
	if err != nil {
		return "", err
	}

	err = w.Close()
	if err != nil {
		return "", err
	}

	return id, nil
}
