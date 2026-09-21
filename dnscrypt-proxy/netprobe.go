package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"time"

	"github.com/jedisct1/dlog"
)

func DeadlineInterval(
	ideal time.Duration,
	deadline time.Time,
	margin time.Duration,
) (interval time.Duration, count int, ok bool) {
	remaining := time.Until(deadline) - margin

	if ideal <= 0 || remaining <= 0 {
		return 0, 0, false
	}

	// Smallest number of intervals that does not require
	// an interval larger than the ideal.
	count = int((remaining + ideal - time.Nanosecond) / ideal)

	interval = remaining / time.Duration(count)

	if interval <= 0 {
		return 0, 0, false
	}

	return interval, count, true
}

func NetProbe(
	proxy *Proxy,
	addresses []netip.AddrPort,
	timeout time.Duration,
) error {
	if len(addresses) == 0 || timeout == 0 {
		return nil
	}
	if captivePortalHandler, err := ColdStart(proxy); err == nil {
		if captivePortalHandler != nil {
			defer captivePortalHandler.Stop()
		}
	} else {
		dlog.Critical(err)
	}

	if timeout < 0 || timeout > MaxTimeout {
		timeout = MaxTimeout
	}

	ctx, cancelDial := context.WithTimeout(context.Background(), timeout)
	defer cancelDial()

	type result struct {
		address netip.AddrPort
		ok      bool
		err     error
	}

	results := make(chan result, len(addresses))

	var probesPending int = 0
	for _, address := range addresses {
		if !address.IsValid() {
			continue
		}

		probesPending++
		go func(address netip.AddrPort) {
			ok, err := NetProbeSingle(proxy, address, ctx)
			results <- result{
				address: address,
				ok:      ok,
				err:     err,
			}
			if ok {
				cancelDial()
			}
		}(address)
	}
	if probesPending == 0 {
		dlog.Error(
			"netprobe_addresses non-zero length but all addresses are invalid somehow",
		)
		return nil
	}

	for {
		select {
		case res := <-results:
			if res.ok && res.err == nil {
				dlog.Noticef(
					"Network connectivity detected (%s)",
					res.address.String(),
				)
				return nil
			} else if !errors.Is(res.err, context.Canceled) {
				dlog.Debugf("(%s) %v", res.address.String(), res.err)
			}

			probesPending--
			if probesPending <= 0 {
				dlog.Error("Timeout while waiting for network connectivity")
				return nil
			}

		}
	}
}

func NetProbeSingle(
	proxy *Proxy,
	address netip.AddrPort,
	ctx context.Context,
) (ok bool, err error) {
	if !address.IsValid() { return false, nil }
	if ctx.Err() != nil { return false, ctx.Err() }

	remoteUDPAddr := net.UDPAddrFromAddrPort(address)

	retried := false

	dialer := net.Dialer{
		Timeout: proxy.timeout,
	}

	deadline, deadlineOk := ctx.Deadline()

	interval := time.Second

	if deadlineOk {
		if i, _, ok := DeadlineInterval(
			time.Second,
			deadline,
			10*time.Millisecond,
		); ok {
			interval = i
		}
	}

	for {
		startTimer := time.NewTimer(interval)

		pc, err := dialer.DialContext(
			ctx,
			"udp",
			remoteUDPAddr.String(),
		)
		if runtime.GOOS == "windows" && err == nil {
			// Write at least 1 byte. This ensures that sockets are ready to use for writing.
			// Windows specific: during the system startup, sockets can be created but the underlying buffers may not be
			// set up yet. If this is the case Write fails with WSAENOBUFS: "An operation on a socket could not be
			// performed because the system lacked sufficient buffer space or because a queue was full"
			_, err = pc.Write([]byte{0})
			if err != nil {
				pc.Close()
			}
		}

		if err != nil {
			if !retried {
				retried = true
				dlog.Noticef(
					"(%s) Network not available yet -- waiting...",
					address.String(),
				)
			}
			dlog.Debugf(
				"(%s) %v",
				address.String(),
				err,
			)

			select {
			case <-ctx.Done():
				dlog.Debugf(
					"(%s) context done",
					address.String(),
				)
				return false, ctx.Err()
			case <-startTimer.C:
			}

			continue
		}
		pc.Close()
		return true, nil
	}
}
