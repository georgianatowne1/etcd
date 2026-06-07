// watchGrpcStream represents a gRPC watch stream.
type watchGrpcStream struct {
    // ...
    lastReceivedRevision int64
    // ...
}

func (s *watchGrpcStream) reconnect() error {
    // ...
    req := &pb.WatchCreateRequest{
        // Set startRevision to the last successfully processed revision
        StartRevision: s.lastReceivedRevision + 1,
    }
    // ...
}

func (s *watchGrpcStream) processEvent(event *pb.WatchEvent) error {
    // Update lastReceivedRevision
    s.lastReceivedRevision = event.Header.Revision
    // ...
}

func (s *watchGrpcStream) handleCompactedError(err error) error {
    // If the requested revision has already been compacted, return ErrCompacted
    if err == context.Canceled || err == context.DeadlineExceeded {
        return ErrCompacted
    }
    // ...
}