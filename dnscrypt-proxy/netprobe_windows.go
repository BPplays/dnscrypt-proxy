//go:build windows

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/jedisct1/dlog"
)


func NetProbeSingle(
	proxy *Proxy,
	address netip.AddrPort,
	timeout time.Duration,
	ctx context.Context,
) error {
	if !address.IsValid() || timeout == 0 {
		return nil
	}

	remoteUDPAddr := net.UDPAddrFromAddrPort(address)

	retried := false
	if timeout < 0 || timeout > MaxTimeout {
		timeout = MaxTimeout
	}

	dialer := net.Dialer{
		Timeout: proxy.timeout,
	}

	for tries := int(timeout / time.Second); tries > 0; tries-- {
		startTimer := time.NewTimer(time.Second)
		defer startTimer.Stop()

		pc, err := dialer.DialContext(
			ctx,
			"udp",
			remoteUDPAddr.String(),
		)
		if err == nil {
			// Write at least 1 byte. This ensures that sockets are ready to use for writing.
			// Windows specific: during the system startup, sockets can be created but the underlying buffers may not be
			// set up yet. If this is the case Write fails with WSAENOBUFS: "An operation on a socket could not be
			// performed because the system lacked sufficient buffer space or because a queue was full"
			_, err = pc.Write([]byte{0})
			if err != nil {
				pc.Close()
			}
		}

		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			dlog.Debugf(
				"(%s) context done",
				address.String(),
			)
			return context.Canceled
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
				return ctx.Err()
			case <-startTimer.C:
			}
			continue
		}
		pc.Close()
		return nil
	}
	return fmt.Errorf(
		"(%s) Timeout while waiting for network connectivity",
		address.String(),
	)
}
