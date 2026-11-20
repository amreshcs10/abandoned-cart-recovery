package com.example.cartrecovery.engine;

import com.example.cartrecovery.config.Config;
import com.example.cartrecovery.model.CartEvent;
import com.example.cartrecovery.model.CartState;
import com.example.cartrecovery.model.CartStatus;
import com.example.cartrecovery.model.EventType;
import com.example.cartrecovery.model.Item;
import com.example.cartrecovery.model.Outcome;
import com.example.cartrecovery.model.Timer;
import com.example.cartrecovery.store.CartStateStore;
import com.example.cartrecovery.store.TimerStore;
import com.example.cartrecovery.stream.OutcomeLog;

/**
 * Consumes {@code cart-events} (partitioned by cartId, so per-cart order holds): drops duplicate
 * and stale events, applies the change, bumps the cart version, and re-arms the cart's single
 * DETECT timer.
 */
public final class AbandonmentDetector {
    private final Config config;
    private final CartStateStore carts;
    private final TimerStore timers;
    private final OutcomeLog outcomes;

    public AbandonmentDetector(Config config, CartStateStore carts, TimerStore timers, OutcomeLog outcomes) {
        this.config = config;
        this.carts = carts;
        this.timers = timers;
        this.outcomes = outcomes;
    }

    public void handle(CartEvent ev) {
        CartState cart = carts.getOrCreate(ev.cartId());

        // Idempotency + ordering: a duplicate (same seq) or a late, older event changes nothing.
        if (ev.seq() <= cart.lastSeq) {
            outcomes.record(cart.cartId, Outcome.Type.EVENT_IGNORED_STALE_OR_DUP, -1, -1, ev.type() + " seq=" + ev.seq());
            return;
        }
        if (cart.status.closed()) {
            cart.lastSeq = ev.seq();
            outcomes.record(cart.cartId, Outcome.Type.EVENT_IGNORED_CLOSED, -1, -1, ev.type().name());
            return;
        }

        cart.lastSeq = ev.seq();
        cart.version++; // fences every timer created for earlier versions
        if (ev.userId() != null) cart.userId = ev.userId();

        if (EventType.EDITS.contains(ev.type())) {
            applyItem(cart, ev);
            if (cart.items.isEmpty()) {
                close(cart, CartStatus.CLEARED); // removing the last item = clear
            } else {
                markActive(cart, ev.occurredAt());
            }
            return;
        }
        switch (ev.type()) {
            // Shopper came back: stop the running sequence and restart the inactivity clock. A new
            // sequence starts only after another full window, and the per-cart cap still applies.
            case CART_RESUMED -> markActive(cart, ev.occurredAt());
            case CART_CLEARED -> close(cart, CartStatus.CLEARED);
            case CHECKOUT_COMPLETED -> close(cart, CartStatus.PURCHASED);
            case CART_MERGED -> close(cart, CartStatus.MERGED);
            default -> throw new IllegalStateException("unhandled " + ev.type());
        }
    }

    private static void applyItem(CartState cart, CartEvent ev) {
        if (ev.type() == EventType.ITEM_REMOVED || ev.qty() <= 0) {
            cart.items.remove(ev.sku());
        } else {
            String name = ev.name() == null || ev.name().isEmpty() ? ev.sku() : ev.name();
            cart.items.put(ev.sku(), new Item(name, ev.qty(), ev.price()));
        }
    }

    private void markActive(CartState cart, long at) {
        cart.status = CartStatus.ACTIVE;
        cart.lastActivity = at;
        // Inactivity is measured from EVENT time, not processing time, so consumer lag never
        // stretches the window. If we are already past the deadline, the timer is due at once.
        timers.schedule(Timer.detect(cart.cartId, cart.version, at + config.inactivityWindowS()));
        outcomes.record(cart.cartId, Outcome.Type.ACTIVITY, cart.version);
    }

    private void close(CartState cart, CartStatus status) {
        cart.status = status;
        // No timer deletion: the version bump makes every pending timer for this cart a no-op.
        outcomes.record(cart.cartId, Outcome.Type.closed(status), cart.version);
    }
}
