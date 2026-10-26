package buf_test

import (
	"crypto/tls"
	"io"
	"testing"
	"time"

	. "github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestWriterCreation(t *testing.T) {
	tcpServer := tcp.Server{}
	dest, err := tcpServer.Start()
	if err != nil {
		t.Fatal("failed to start tcp server: ", err)
	}
	defer tcpServer.Close()

	conn, err := net.Dial("tcp", dest.NetAddr())
	if err != nil {
		t.Fatal("failed to dial a TCP connection: ", err)
	}
	defer conn.Close()

	{
		writer := NewWriter(conn)
		if _, ok := writer.(*BufferToBytesWriter); !ok {
			t.Fatal("writer is not a BufferToBytesWriter")
		}

		writer2 := NewWriter(writer.(io.Writer))
		if writer2 != writer {
			t.Fatal("writer is not reused")
		}
	}

	tlsConn := tls.Client(conn, &tls.Config{
		InsecureSkipVerify: true,
	})
	defer tlsConn.Close()

	{
		writer := NewWriter(tlsConn)
		if _, ok := writer.(*SequentialWriter); !ok {
			t.Fatal("writer is not a SequentialWriter")
		}
	}
}

func TestTimeoutWrapperReaderForcedErrorWakesBlockedPipeRead(t *testing.T) {
	pr, pw := pipe.New()
	r := &TimeoutWrapperReader{Reader: pr}

	result := make(chan error, 1)
	go func() {
		_, err := r.ReadMultiBuffer()
		result <- err
	}()
	time.Sleep(50 * time.Millisecond) // let the read block inside the pipe
	r.ReturnAnError(io.EOF)
	select {
	case err := <-result:
		if err != io.EOF {
			t.Fatalf("blocked read returned %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("forced error did not wake the blocked read")
	}
	if err := r.Recover(); err != nil {
		t.Fatalf("consumed forced error was still pending: %v", err)
	}

	// An unconsumed forced error is cleared by Recover and the link stays usable.
	r.ReturnAnError(io.EOF)
	if err := r.Recover(); err != io.EOF {
		t.Fatalf("Recover returned %v, want EOF", err)
	}
	b := New()
	b.WriteString("x")
	if err := pw.WriteMultiBuffer(MultiBuffer{b}); err != nil {
		t.Fatal(err)
	}
	mb, err := r.ReadMultiBuffer()
	if err != nil || mb.Len() != 1 {
		t.Fatalf("read after recovery: len=%d err=%v", mb.Len(), err)
	}
	ReleaseMulti(mb)
}
