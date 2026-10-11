package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// Exercise actual transport ping enforcement, without application messages
// resetting the strike counter. Default grpc-go fails this after three pings.
func TestNativeGamesyncPingInterval(t *testing.T) {
	s := newServer(&serverContext{
		writer:      io.Discard,
		identityKey: bytes.Repeat([]byte{1}, 32),
	})

	s.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Pings", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Park", ServerStreams: true, ClientStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
		<-stream.Context().Done()

		return stream.Context().Err()
	}}}}, struct{}{})

	l := bufconn.Listen(65536)

	go s.Serve(l)
	defer s.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 32*time.Second)
	defer cancel()

	c, err := l.DialContext(ctx)

	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.SetDeadline(time.Now().Add(32 * time.Second))

	f := http2.NewFramer(c, c)

	io.WriteString(c, http2.ClientPreface)

	if err := f.WriteSettings(); err != nil {
		t.Fatal(err)
	}

	var headers bytes.Buffer

	e := hpack.NewEncoder(&headers)

	for _, field := range []hpack.HeaderField{
		{
			Name:  ":method",
			Value: "POST",
		},
		{
			Name:  ":scheme",
			Value: "http",
		},
		{
			Name:  ":path",
			Value: "/test.Pings/Park",
		},
		{
			Name:  ":authority",
			Value: "test",
		},
		{
			Name:  "content-type",
			Value: "application/grpc",
		},
		{
			Name:  "te",
			Value: "trailers",
		},
	} {
		e.WriteField(field)
	}

	if err := f.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      1,
		BlockFragment: headers.Bytes(),
		EndHeaders:    true,
	}); err != nil {
		t.Fatal(err)
	}

	acks := make(chan [8]byte, 8)
	failures := make(chan error, 1)

	go func() {
		reader := http2.NewFramer(nil, c)

		for {
			frame, err := reader.ReadFrame()

			if err != nil {
				failures <- err

				return
			}

			switch x := frame.(type) {
			case *http2.GoAwayFrame:
				failures <- &pingGoAway{
					string(x.DebugData()),
				}
				return

			case *http2.PingFrame:
				if x.IsAck() {
					acks <- x.Data
				}
			}
		}
	}()

	for i := byte(1); i <= 5; i++ {
		timer := time.NewTimer(5100 * time.Millisecond)

		select {
		case <-timer.C:

		case err := <-failures:
			timer.Stop()
			t.Fatal(err)
		}

		data := [8]byte{
			i,
		}

		if err := f.WritePing(false, data); err != nil {
			t.Fatal(err)
		}

		select {
		case ack := <-acks:
			if ack != data {
				t.Fatal("incorrect ping acknowledgement")
			}

		case err := <-failures:
			t.Fatal(err)

		case <-ctx.Done():
			t.Fatal("missing ping acknowledgement")
		}
	}
}

type pingGoAway struct{ reason string }

func (e *pingGoAway) Error() string {
	return "unexpected transport GOAWAY: " + e.reason
}
