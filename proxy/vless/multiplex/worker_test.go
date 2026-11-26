package multiplex_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/proxy/vless/multiplex"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestWorkerCloseInterruptsUnderlyingWriter(t *testing.T) {
	for _, role := range []string{"client", "server"} {
		t.Run(role, func(t *testing.T) {
			inputReader, inputWriter := pipe.New()
			outputReader, outputWriter := pipe.New()
			defer inputWriter.Interrupt()
			defer outputWriter.Interrupt()
			link := transport.Link{Reader: inputReader, Writer: outputWriter}

			var worker io.Closer
			if role == "client" {
				worker = multiplex.NewClientWorker(context.Background(), link, multiplex.ClientStrategy{})
			} else {
				worker = multiplex.NewServerWorker(context.Background(), nil, &link, nil)
			}
			if err := worker.Close(); err != nil {
				t.Fatal(err)
			}

			mb, err := outputReader.ReadMultiBufferTimeout(time.Second)
			buf.ReleaseMulti(mb)
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("reading the underlying pipe after worker.Close(): got %v, want %v", err, io.ErrClosedPipe)
			}
		})
	}
}
