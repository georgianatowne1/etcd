func TestWatchGrpcStream_Reconnect(t *testing.T) {
    // Test reconnect logic with leader election
    // ...
    req := &pb.WatchCreateRequest{
        StartRevision: 10,
    }
    stream, err := client.Watch(context.Background(), req)
    if err != nil {
        t.Fatal(err)
    }
    // Simulate leader election
    // ...
    // Verify that the watch stream resumes from the correct revision
    event, err := stream.Recv()
    if err != nil {
        t.Fatal(err)
    }
    if event.Header.Revision != 11 {
        t.Errorf("expected revision 11, got %d", event.Header.Revision)
    }
}

func TestWatchGrpcStream_CompactedError(t *testing.T) {
    // Test ErrCompacted error handling
    // ...
    req := &pb.WatchCreateRequest{
        StartRevision: 100,
    }
    stream, err := client.Watch(context.Background(), req)
    if err != nil {
        t.Fatal(err)
    }
    // Simulate compaction of requested revision
    // ...
    // Verify that ErrCompacted is returned
    _, err = stream.Recv()
    if err != ErrCompacted {
        t.Errorf("expected ErrCompacted, got %v", err)
    }
}