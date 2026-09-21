package main

import (
	"context"
	"errors"
	"net/netip"
	"time"
	"fmt"

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
	count = int((remaining + ideal - 1) / ideal)

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
	start := time.Now()
	ctx, cancelDial := context.WithDeadline(
		context.Background(),
		time.Now().Add(timeout),
	)
	defer cancelDial()

	type result struct {
		address netip.AddrPort
		ok      bool
		err     error
	}

	results := make(chan result, len(addresses))

	var probes_pending int = 0
	for _, address := range addresses {
		if !address.IsValid() {
			continue
		}

		probes_pending++
		go func(address netip.AddrPort) {
			ok, err := NetProbeSingle(proxy, address, ctx)
			results <- result{
				address:  address,
				ok:       ok,
				err:      err,
			}
			if ok {
				cancelDial()
			}
		}(address)
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

			probes_pending--
			if probes_pending <= 0 {
				elapsed := time.Since(start)
				fmt.Printf("took %v\n", elapsed)
				dlog.Error("Timeout while waiting for network connectivity")
				return nil
			}

		}
	}
}

