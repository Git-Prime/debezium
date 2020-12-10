package io.debezium.connector.postgresql.connection;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

class PostgresReplicationStream implements ReplicationStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReplicationStream.class);

    private static final int CHECK_WARNINGS_AFTER_COUNT = 100;
    private final PostgresReplicationConnection postgresReplicationConnection;
    private final PGReplicationStream stream;
    private final Lsn startLsn;
    private final WalPositionLocator walPosition;
    private int warningCheckCounter;
    private ExecutorService keepAliveExecutor;
    private AtomicBoolean keepAliveRunning;
    private final Metronome metronome;
    private final MessageDecoder messageDecoder;
    private final TypeRegistry typeRegistry;

    // make sure this is volatile since multiple threads may be interested in this value
    private volatile Lsn lastReceivedLsn;

    public PostgresReplicationStream(PostgresReplicationConnection postgresReplicationConnection,
                                     PGReplicationStream stream,
                                     Lsn startLsn,
                                     WalPositionLocator walPosition,
                                     Duration statusUpdateInterval,
                                     MessageDecoder messageDecoder,
                                     TypeRegistry typeRegistry) {
        this.postgresReplicationConnection = postgresReplicationConnection;
        this.stream = stream;
        this.startLsn = startLsn;
        this.walPosition = walPosition;
        this.messageDecoder = messageDecoder;
        this.typeRegistry = typeRegistry;
        warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;
        keepAliveExecutor = null;
        metronome = Metronome.sleeper(statusUpdateInterval, Clock.SYSTEM);
    }

    @Override
    public void read(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        ByteBuffer read = stream.read();
        final Lsn lastReceiveLsn = Lsn.valueOf(stream.getLastReceiveLSN());
        LOGGER.trace("Streaming requested from LSN {}, received LSN {}", startLsn, lastReceiveLsn);
        if (messageDecoder.shouldMessageBeSkipped(read, lastReceiveLsn, startLsn, walPosition)) {
            return;
        }
        deserializeMessages(read, processor);
    }

    @Override
    public boolean readPending(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        ByteBuffer read = stream.readPending();
        final Lsn lastReceiveLsn = Lsn.valueOf(stream.getLastReceiveLSN());
        LOGGER.trace("Streaming requested from LSN {}, received LSN {}", startLsn, lastReceiveLsn);

        if (read == null) {
            return false;
        }

        if (messageDecoder.shouldMessageBeSkipped(read, lastReceiveLsn, startLsn, walPosition)) {
            return true;
        }

        deserializeMessages(read, processor);

        return true;
    }

    private void deserializeMessages(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        lastReceivedLsn = Lsn.valueOf(stream.getLastReceiveLSN());
        LOGGER.trace("Received message at LSN {}", lastReceivedLsn);
        messageDecoder.processMessage(buffer, processor, typeRegistry);
    }

    @Override
    public void close() throws SQLException {
        processWarnings(true);
        stream.close();
    }

    @Override
    public void flushLsn(Lsn lsn) throws SQLException {
        doFlushLsn(lsn);
    }

    private void doFlushLsn(Lsn lsn) throws SQLException {
        stream.setFlushedLSN(lsn.asLogSequenceNumber());
        stream.setAppliedLSN(lsn.asLogSequenceNumber());

        stream.forceUpdateStatus();
    }

    @Override
    public Lsn lastReceivedLsn() {
        return lastReceivedLsn;
    }

    @Override
    public void startKeepAlive(ExecutorService service) {
        if (keepAliveExecutor == null) {
            keepAliveExecutor = service;
            keepAliveRunning = new AtomicBoolean(true);
            keepAliveExecutor.submit(() -> {
                while (keepAliveRunning.get()) {
                    try {
                        LOGGER.trace("Forcing status update with replication stream");
                        stream.forceUpdateStatus();

                        metronome.pause();
                    } catch (Exception exp) {
                        throw new RuntimeException("received unexpected exception will perform keep alive", exp);
                    }
                }
            });
        }
    }

    @Override
    public void stopKeepAlive() {
        if (keepAliveExecutor != null) {
            keepAliveRunning.set(false);
            keepAliveExecutor.shutdownNow();
            keepAliveExecutor = null;
        }
    }

    private void processWarnings(final boolean forced) throws SQLException {
        if (--warningCheckCounter == 0 || forced) {
            warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;
            for (SQLWarning w = postgresReplicationConnection.connection().getWarnings(); w != null; w = w.getNextWarning()) {
                LOGGER.debug("Server-side message: '{}', state = {}, code = {}",
                        w.getMessage(), w.getSQLState(), w.getErrorCode());
            }
        }
    }

    @Override
    public Lsn startLsn() {
        return startLsn;
    }
}
