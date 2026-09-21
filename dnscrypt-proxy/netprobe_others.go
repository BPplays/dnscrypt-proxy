//go:build !windows

package main

import (
	"context"
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
