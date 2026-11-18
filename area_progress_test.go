package chunkdb

import (
	"context"
	"errors"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestAreaProgressExactPrefixes(t *testing.T) {
	for _, dims := range []int{2, 3} {
		for _, ball := range []bool{false, true} {
			request := areaPageRequest{dims: dims, first: [3]int64{-1, -1, -1}, last: [3]int64{1, 1, 1}}
			if ball {
				radius := uint64(2)
				request.first, request.radius = [3]int64{}, &radius
			}
			var points [][3]int64
			for x := int64(-2); x <= 2; x++ {
				for y := int64(-2); y <= 2; y++ {
					for z := int64(-2); z <= 2; z++ {
						if dims == 2 && z != 0 {
							continue
						}
						inside := x >= -1 && x <= 1 && y >= -1 && y <= 1 && (dims == 2 || z >= -1 && z <= 1)
						if ball {
							inside = x*x+y*y+z*z <= 4
						}
						if inside {
							points = append(points, [3]int64{x, y, z})
						}
					}
				}
			}
			for start := 0; start <= len(points); start++ {
				request.after = nil
				if start != 0 {
					request.after = &points[start-1]
				}
				for limit := 1; limit <= len(points)+1; limit++ {
					for end := start; end <= len(points); end++ {
						var cursor *[3]int64
						if end != len(points) {
							cursor = &points[end]
						}
						count := end - start + 1
						if cursor == nil {
							count--
						}
						want := count <= limit
						if cursor != nil && end == len(points)-1 {
							want = false
						}
						got, err := request.validProgress(cursor, uint64(limit), nil)
						if err != nil || got != want {
							t.Fatalf("dims=%d ball=%t start=%d end=%d limit=%d: %t %v, want %t", dims, ball, start, end, limit, got, err, want)
						}
					}
				}
			}
		}
	}
}

func TestAreaBoxProgressHasConstantWork(t *testing.T) {
	request := areaPageRequest{dims: 3, first: [3]int64{math.MinInt64, math.MinInt64, math.MinInt64}, last: [3]int64{math.MaxInt64, math.MaxInt64, math.MaxInt64}}
	check := func(cursor *[3]int64, limit uint64, want bool) {
		t.Helper()
		calls := 0
		got, err := request.validProgress(cursor, limit, func() error {
			calls++
			if calls > 2 {
				return errors.New("rectangle validation enumerated cells")
			}
			return nil
		})
		if err != nil || got != want || calls != 2 {
			t.Fatal(got, err, calls, want)
		}
	}
	check(nil, math.MaxUint64, false)
	request.after = &[3]int64{math.MaxInt64, math.MaxInt64, math.MaxInt64 - 1}
	check(nil, 1, true)
	request.after = &[3]int64{math.MaxInt64, math.MaxInt64, math.MaxInt64 - 2}
	check(&[3]int64{math.MaxInt64, math.MaxInt64, math.MaxInt64 - 1}, 1, true)
	request = areaPageRequest{dims: 2, first: [3]int64{math.MinInt64, 0}, last: [3]int64{math.MaxInt64, 0}}
	check(nil, math.MaxUint64, false)
	request.after = &[3]int64{math.MinInt64, 0}
	check(nil, math.MaxUint64, true)
}

func TestAreaBallProgressChecksWholeRowsAndDeadline(t *testing.T) {
	radius := uint64(2)
	for _, dims := range []int{2, 3} {
		request := areaPageRequest{dims: dims, radius: &radius}
		rows, cells := 5, 13
		if dims == 3 {
			rows, cells = 13, 33
		}
		calls := 0
		valid, err := request.validProgress(nil, uint64(cells), func() error { calls++; return nil })
		if err != nil || !valid || calls != rows+2 {
			t.Fatal(dims, valid, err, calls)
		}
		calls = 0
		_, err = request.validProgress(nil, uint64(cells), func() error {
			calls++
			if calls == 3 {
				return context.DeadlineExceeded
			}
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || calls != 3 {
			t.Fatal(err, calls)
		}
	}
}

func TestAreaDecodeRetainsAndReleasesOriginalDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	control := &areaResponseControl{deadline: callDeadline{parent: context.Background(), ctx: ctx, cancel: cancel}}
	value, err := decodeAreaResponse(control, func() (int, error) {
		if ctx.Err() != nil {
			t.Fatal("deadline canceled before typed decode")
		}
		return 7, nil
	})
	if value != 7 || err != nil || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal(value, err, ctx.Err())
	}
	ctx, cancel = context.WithCancel(context.Background())
	control.deadline.ctx, control.deadline.cancel = ctx, cancel
	_, err = decodeAreaResponse(control, func() (int, error) { return 0, &staleSchemaError{err: protocolErrorf("GET AREA", "refresh")} })
	var stale *staleSchemaError
	if !errors.As(err, &stale) || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal(err, ctx.Err())
	}
}

func TestAreaBallProgressInterruptsBetweenRows(t *testing.T) {
	radius := uint64(2)
	request := areaPageRequest{dims: 3, radius: &radius}
	calls := 0
	valid, err := request.validProgress(nil, 33, func() error {
		calls++
		if calls == 3 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if valid || !errors.Is(err, context.DeadlineExceeded) || calls != 3 {
		t.Fatal(valid, err, calls)
	}
}

func TestAreaDecodeExpiredDeadlineClosesOnlySource(t *testing.T) {
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	owner := &Client{}
	current := &conn{}
	owner.active = current
	old := &conn{client: owner, netConn: source}
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	control := &areaResponseControl{connection: old, deadline: callDeadline{parent: ctx, ctx: ctx, cancel: cancel, command: "GET AREA"}}
	called := false
	value, err := decodeAreaResponse(control, func() (int, error) { called = true; return 9, nil })
	if value != 0 || called || !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) || !old.failed || owner.active != current {
		t.Fatal(value, called, err, old.failed, owner.active == current)
	}
	if _, err = peer.Write([]byte("must be closed")); err == nil {
		t.Fatal("expired source remained open")
	}
}

func TestAreaCallTransfersOriginalDeadline(t *testing.T) {
	server := newFakeServer(t, func(_ *fakeServer, connection net.Conn, line string) {
		if commandOf(line) == "HELLO" {
			writeRaw(connection, areaPageHello(64<<20))
			return
		}
		writeRaw(connection, areaPageWire(nil, false, nil))
	})
	client := newTestClient(t, server, func(options *Options) { options.CommandTimeout = time.Hour })
	end := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(t.Context(), end)
	defer cancel()
	var control *areaResponseControl
	_, err := client.call(ctx, "GET AREA 0 0 TO 0 0 FROM space LIMIT 1", nil, areaReplyOptions{knownShape: true, page: true, limit: 1, response: &control})
	if err != nil || control == nil {
		t.Fatal(err, control)
	}
	deadline, exists := control.deadline.ctx.Deadline()
	if !exists || deadline != end || control.deadline.ctx.Err() != nil || control.connection == nil {
		t.Fatal(deadline, end, control.deadline.ctx.Err())
	}
	_, err = decodeAreaResponse(control, func() (int, error) { return 7, nil })
	if err != nil || !errors.Is(control.deadline.ctx.Err(), context.Canceled) || ctx.Err() != nil {
		t.Fatal(err, control.deadline.ctx.Err(), ctx.Err())
	}
}

func TestAreaDecodeCancellationDuringCallbackClosesOnlySource(t *testing.T) {
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	owner, replacement := &Client{}, &conn{}
	owner.active = replacement
	connection := &conn{client: owner, netConn: source}
	ctx, cancel := context.WithCancel(t.Context())
	control := &areaResponseControl{connection: connection, deadline: callDeadline{parent: ctx, ctx: ctx, cancel: cancel, command: "GET AREA"}}
	value, err := decodeAreaResponse(control, func() (int, error) {
		cancel()
		return 9, nil
	})
	if value != 0 || !errors.Is(err, ErrTimeout) || !errors.Is(err, context.Canceled) || !connection.failed || owner.active != replacement {
		t.Fatal(value, err, connection.failed, owner.active == replacement)
	}
	if _, err := peer.Write([]byte("closed")); err == nil {
		t.Fatal("expired source remained open")
	}
}

func TestAreaDecodeCaughtTimeoutReleasesPoolAndPreventsCommit(t *testing.T) {
	for _, mode := range []string{"pool", "transaction", "pool-transaction"} {
		t.Run(mode, func(t *testing.T) {
			var commits atomic.Int32
			server := newFakeServer(t, func(_ *fakeServer, connection net.Conn, line string) {
				switch commandOf(line) {
				case "HELLO":
					writeRaw(connection, areaPageHello(64<<20))
				case "GET AREA":
					writeRaw(connection, areaPageWire(nil, false, nil))
				case "PING":
					writeSimple(connection, "PONG")
				case "COMMIT":
					commits.Add(1)
					writeRaw(connection, respNull)
				default:
					writeSimple(connection, "OK")
				}
			})
			caught := func(caller session) error {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var control *areaResponseControl
				_, err := caller.call(ctx, "GET AREA 0 0 TO 0 0 FROM space LIMIT 1", nil, areaReplyOptions{knownShape: true, page: true, limit: 1, response: &control})
				if err != nil {
					return err
				}
				_, err = decodeAreaResponse(control, func() (int, error) { cancel(); return 7, nil })
				if !errors.Is(err, ErrTimeout) || !control.connection.failed {
					t.Fatal(err, control.connection.failed)
				}
				return nil // Deliberately catch the timeout inside the owner callback.
			}
			if mode == "transaction" {
				client := newTestClient(t, server, nil)
				if _, err := client.Transaction(t.Context(), func(tx *Tx) error { return caught(tx) }); !errors.Is(err, ErrTimeout) {
					t.Fatal(err)
				}
				prior := server.acceptedConns()
				if err := client.Ping(t.Context()); err != nil || server.acceptedConns() != prior+1 {
					t.Fatal(err, server.acceptedConns(), prior)
				}
			} else {
				pool := newTestPool(t, server, func(options *PoolOptions) { options.MaxConnections = 1 })
				if mode == "pool" {
					if err := pool.WithClient(t.Context(), func(_ context.Context, client *Client) error { return caught(client) }); err != nil {
						t.Fatal(err)
					}
				} else if _, err := pool.Transaction(t.Context(), func(tx *Tx) error { return caught(tx) }); !errors.Is(err, ErrTimeout) {
					t.Fatal(err)
				}
				prior := server.acceptedConns()
				if err := pool.Ping(t.Context()); err != nil || server.acceptedConns() != prior+1 {
					t.Fatal(err, server.acceptedConns(), prior)
				}
			}
			if commits.Load() != 0 {
				t.Fatal("caught decoding timeout committed a transaction")
			}
		})
	}
}
