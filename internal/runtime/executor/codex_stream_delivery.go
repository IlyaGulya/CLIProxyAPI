package executor

import cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"

type codexStreamDeliveryHooks struct {
	Deliver    func(cliproxyexecutor.StreamChunk) bool
	Committed  func(codexTransactionalDrain)
	Discarded  func(int)
	Overflowed func(int)
}

// codexStreamDelivery owns the ordering between transactional buffering and
// irreversible downstream delivery. Observability stays outside through hooks.
type codexStreamDelivery struct {
	bridge *codexWebsocketStreamBridge
	hooks  codexStreamDeliveryHooks
}

func newCodexStreamDelivery(bridge *codexWebsocketStreamBridge, hooks codexStreamDeliveryHooks) *codexStreamDelivery {
	return &codexStreamDelivery{bridge: bridge, hooks: hooks}
}

func (d *codexStreamDelivery) flush() bool {
	drain := d.bridge.drain()
	for i := range drain.Chunks {
		if !d.hooks.Deliver(drain.Chunks[i]) {
			return false
		}
	}
	if drain.Bytes > 0 && d.hooks.Committed != nil {
		d.hooks.Committed(drain)
	}
	if d.bridge.transactionState() == codexTransactionBuffering {
		d.bridge.commitTransaction()
	}
	return true
}

func (d *codexStreamDelivery) send(chunk cliproxyexecutor.StreamChunk) bool {
	if chunk.Err != nil {
		if discardedBytes := d.bridge.discardTransaction(); discardedBytes > 0 && d.hooks.Discarded != nil {
			d.hooks.Discarded(discardedBytes)
		}
		return d.hooks.Deliver(chunk)
	}
	stage := d.bridge.stage(chunk)
	if stage.Buffered {
		return true
	}
	if stage.Overflow {
		bufferedBytes := d.bridge.bufferedBytes()
		if d.hooks.Overflowed != nil {
			d.hooks.Overflowed(bufferedBytes + len(chunk.Payload))
		}
		d.bridge.enterPassthrough()
		if !d.flush() {
			return false
		}
	}
	return d.hooks.Deliver(chunk)
}
