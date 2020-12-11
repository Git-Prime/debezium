package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;

public class WalEntry {
    private final Lsn lsn;
    private final ByteBuffer data;

    WalEntry(Lsn lsn, ByteBuffer data) {
        this.lsn = lsn;
        this.data = data;
    }

    public Lsn getLsn() {
        return lsn;
    }

    public ByteBuffer getData() {
        return data;
    }
}
