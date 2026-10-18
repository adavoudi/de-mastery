package de.flk.harness;

import java.io.Serializable;

/** A GMV event: {@code companyId}, {@code amountCents}, event-time {@code tsMs}. */
public class Event implements Serializable {
    public String companyId;
    public long amountCents;
    public long tsMs;

    public Event() {}

    public Event(String companyId, long amountCents, long tsMs) {
        this.companyId = companyId;
        this.amountCents = amountCents;
        this.tsMs = tsMs;
    }
}
