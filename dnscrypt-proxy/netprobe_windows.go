//go:build windows

package main

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/jedisct1/dlog"
)


func NetProbeSingle(
	proxy *Proxy,
	address netip.AddrPort,
	ctx context.Context,
) (ok bool, err error) {
	if !address.IsValid() || ctx.Err() != nil {
		return false, nil
	}

	remoteUDPAddr := net.UDPAddrFromAddrPort(address)

	retried := false

	dialer := net.Dialer{
		Timeout: proxy.timeout,
	}


	deadline, deadlineOk := ctx.Deadline()

	interval := time.Second

	if deadlineOk {
		inter, _, ok := DeadlineInterval(
			time.Second,
			deadline,
			10 * time.Millisecond,
		)
		if ok {
			interval = inter
		}

	}
	fmt.Println(interval)

	for {
		timerDuration := interval
		if deadlineOk {
			timerDuration = min(
				time.Until(deadline),
				interval,
			)
		}

		if timerDuration <= 0 {
			return false, nil
		}

		startTimer := time.NewTimer(timerDuration)

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
				startTimer.Stop()
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
