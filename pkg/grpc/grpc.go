package grpc

import (
	"context"
	"github.com/luminosita/common-bee/pkg/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

type NewRequestFunc = func([]byte) any

type ReadChunkDataFunc = func(reply any) []byte

type MessageStreamer interface {
	SendMsg(m any) error
	RecvMsg(m any) error
	Context() context.Context
}

type Opts struct {
	BufferSize int
}

func initBuffer(opts ...*Opts) []byte {
	bufferSize := 3 * 1024

	if len(opts) > 0 && opts[0] != nil {
		bufferSize = opts[0].BufferSize
	}

	return make([]byte, bufferSize)
}

func CopyToMessageStream(r io.ReadCloser, stream MessageStreamer, nrf NewRequestFunc, opts ...*Opts) error {
	buffer := initBuffer(opts...)

	for {
		n, err := r.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		req := nrf(buffer[:n])

		err = stream.SendMsg(req)
		if err != nil {
			return err
		}
	}

	err := r.Close()
	if err != nil {
		return log.LogError(status.Errorf(codes.Internal,
			"cannot close repository stream: %v", err))
	}

	return nil
}

func CopyFromMessageStream(w io.Writer, stream MessageStreamer, reply any, rcdf ReadChunkDataFunc) (err error) {
	for {
		err = contextError(stream.Context())
		if err != nil {
			return err
		}

		err = stream.RecvMsg(reply)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		chunk := rcdf(reply)

		if len(chunk) > 0 {
			_, err = w.Write(chunk)
			if err != nil {
				return err
			}
		}

		chunk = chunk[:0]
	}

	return nil
}

func contextError(ctx context.Context) error {
	switch ctx.Err() {
	case context.Canceled:
		return log.LogError(status.Error(codes.Canceled, "request is canceled"))
	case context.DeadlineExceeded:
		return log.LogError(status.Error(codes.DeadlineExceeded, "deadline is exceeded"))
	default:
		return nil
	}
}
