package com.example.cartrecovery.model;

import java.util.List;

/**
 * What would be handed to the notification provider. The real send is out of scope; the pipeline
 * shows that this trigger fires, when, and with what content.
 *
 * @param key      idempotency key (= ledger key)
 * @param greeting "Hi {firstName}," or "Hi there,"
 * @param items    top 3 items by line value, plus "and N more"
 */
public record Trigger(String key, String cartId, String recipient, String template, int reminderIndex,
                      String greeting, List<String> items) {}
