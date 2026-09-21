package main

import (
	"context"
	"errors"
	"net/netip"
	"time"

	"github.com/jedisct1/dlog"
)


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

	ctx, cancelDial := context.WithDeadline(
		context.Background(),
		time.Now().Add(timeout).Add(499 * time.Millisecond),
	)
	defer cancelDial()

	type result struct {
		address netip.AddrPort
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
			err := NetProbeSingle(proxy, address, timeout, ctx)
			results <- result{
				address: address,
				err:     err,
			}
			if err == nil {
				cancelDial()
			}
		}(address)
	}

	for {
		select {
		case res := <-results:
			if res.err == nil {
				dlog.Noticef(
					"Network connectivity detected (%s)",
					res.address.String(),
					)
				return nil
			} else if !errors.Is(res.err, context.Canceled) {
				dlog.Debug(res.err)
			}

			probes_pending--
			if probes_pending <= 0 {
				dlog.Error("Timeout while waiting for network connectivity")
				return nil
			}

		}
	}
}

