package grpc

import (
	"context"
	"github.com/luminosita/common-bee/pkg/log"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

type NewRequestFunc = func([]byte) any

type ReadChunkDataFunc = func(reply any) []byte

type Opts struct {
	BufferSize int
}

func CopyToClientStream(r io.ReadCloser, stream grpc.ClientStream, nrf NewRequestFunc, opts ...*Opts) error {
	bufferSize := 3 * 1024

	if len(opts) > 0 && opts[0] != nil {
		bufferSize = opts[0].BufferSize
	}

	buffer := make([]byte, bufferSize)

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

	return nil
}

func CopyFromClientStream(w *io.PipeWriter, stream grpc.ClientStream, reply any, rcdf ReadChunkDataFunc) (err error) {
	for {
		err = contextError(stream.Context())
		if err != nil {
			return err
		}

		err = stream.RecvMsg(reply)
		if err == io.EOF {
			_ = w.Close()
			break
		}
		if err != nil {
			_ = w.CloseWithError(err)
			break
		}

		chunk := rcdf(reply)

		if len(chunk) > 0 {
			_, err = w.Write(chunk)
			if err != nil {
				_ = stream.CloseSend()
				break
			}
		}
		chunk = chunk[:0]
	}

	return
}

func CopyToServerStream(r io.ReadCloser, stream grpc.ServerStream, nrf NewRequestFunc, opts ...*Opts) error {
	bufferSize := 3 * 1024

	if len(opts) > 0 && opts[0] != nil {
		bufferSize = opts[0].BufferSize
	}

	buffer := make([]byte, bufferSize)

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

	//	chunk := &pb.GetDocumentReply_ChunkData{
	//		ChunkData: make([]byte, chunkSize),
	//	}
	//
	//	var n int
	//
	//Loop:
	//	for {
	//		n, err = res.Reader.Read(chunk.ChunkData)
	//		switch err {
	//		case nil:
	//		case io.EOF:
	//			break Loop
	//		default:
	//			return log.LogError(status.Errorf(codes.Internal,
	//				"unable to send chunk data: %v", err))
	//		}
	//		chunk.ChunkData = chunk.ChunkData[:n]
	//		serverErr := srv.Send(&pb.GetDocumentReply{Data: chunk})
	//		if serverErr != nil {
	//			return log.LogError(status.Errorf(codes.Internal,
	//				"server.Send: %v", serverErr))
	//		}
	//	}

}

func CopyFromServerStream(w *io.PipeWriter, stream grpc.ServerStream, reply any, rcdf ReadChunkDataFunc) error {
	for {
		err := contextError(stream.Context())
		if err != nil {
			return err
		}

		err = stream.RecvMsg(reply)
		if err == io.EOF {
			_ = w.Close()
			break
		}
		if err != nil {
			_ = w.CloseWithError(err)
			break
		}

		chunk := rcdf(reply)

		if len(chunk) > 0 {
			_, err = w.Write(chunk)
			if err != nil {
				//				_ = stream.CloseSend()
				break
			}
		}
		//		chunk = chunk[:0]
	}

	return nil

	//for {
	//	err := contextError(srv.Context())
	//	if err != nil {
	//		return err
	//	}
	//
	//	req, err := srv.Recv()
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		return log.LogError(status.Errorf(codes.Unknown, "cannot receive chunk data: %v", err))
	//	}
	//
	//	chunk := req.GetChunkData()
	//	size := len(chunk)
	//
	//	imageSize += size
	//
	//	_, err = res.Writer.Write(chunk)
	//	if err != nil {
	//		return log.LogError(status.Errorf(codes.Internal,
	//			"cannot write chunk data to repository stream: %v", err))
	//	}
	//}
	//
	//err = res.Writer.Close()
	//if err != nil {
	//	return log.LogError(status.Errorf(codes.Internal,
	//		"cannot close repository stream: %v", err))
	//}

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
