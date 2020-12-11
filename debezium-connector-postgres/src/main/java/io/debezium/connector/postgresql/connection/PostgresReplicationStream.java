package io.debezium.connector.postgresql.connection;

import java.net.SocketException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.TypeRegistry;

class PostgresReplicationStream implements ReplicationStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReplicationStream.class);

    private static final int CHECK_WARNINGS_AFTER_COUNT = 100;
    private final PostgresReplicationConnection postgresReplicationConnection;
    private final PGReplicationStream stream;
    private final Lsn startLsn;
    private final WalPositionLocator walPosition;
    private int warningCheckCounter;
    private ScheduledExecutorService executorService;
    private AtomicBoolean threadsRunning;
    private final long statusUpdateIntervalMs;
    private final MessageDecoder messageDecoder;
    private final TypeRegistry typeRegistry;
    private final BlockingQueue<WalEntry> walQueue = new LinkedBlockingQueue<>();

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
        executorService = null;
        this.statusUpdateIntervalMs = statusUpdateInterval.toMillis();
    }

    @Override
    public void read(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        WalEntry entry = walQueue.take();

        deserializeMessages(entry, processor);
    }

    @Override
    public boolean readPending(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        WalEntry entry = walQueue.peek();

        if (entry == null) {
            return false;
        }

        walQueue.remove();

        deserializeMessages(entry, processor);

        return true;
    }

    private void deserializeMessages(WalEntry entry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        lastReceivedLsn = entry.getLsn();
        LOGGER.trace("Received message at LSN {}", lastReceivedLsn);
        messageDecoder.processMessage(entry, processor, typeRegistry);
    }

    @Override
    public void close() throws SQLException {
        processWarnings(true);
        stopThreads();
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
    public void startThreads(ScheduledExecutorService service) {
        if (executorService == null) {
            executorService = service;
            threadsRunning = new AtomicBoolean(true);
            executorService.scheduleAtFixedRate(this::runKeepAlive, statusUpdateIntervalMs, statusUpdateIntervalMs, TimeUnit.MILLISECONDS);
            executorService.scheduleWithFixedDelay(this::runBufferingThread, 1, 1, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stopThreads() {
        if (executorService != null) {
            threadsRunning.set(false);
            executorService.shutdownNow();
            executorService = null;
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

    /** Send regular status messages to the server.
     * This thread runs every statusUpdateIntervalMs in order to make sure the server knows we're still alive.
     */
    private void runKeepAlive() {
        try {
            LOGGER.trace("Forcing status update with replication stream");
            stream.forceUpdateStatus();
        }
        catch (Exception exp) {
            throw new RuntimeException("received unexpected exception will perform keep alive", exp);
        }
    }

    /** Check for new messages from the server.
     * This thread checks for new messages from the server in order to make sure our incoming TCP buffers
     * do not fill up and we have responded to any keep-alive messages from the server. Otherwise it may disconnect
     * us.
     */
    private void runBufferingThread() {
        try {
            // TODO check that we are connected and reconnect if not?
            ByteBuffer read = stream.readPending();
            while (read != null) {
                final Lsn lastReceivedLsn = Lsn.valueOf(stream.getLastReceiveLSN());
                LOGGER.trace("Streaming requested from LSN {}, received LSN {}", startLsn, lastReceivedLsn);

                if (!messageDecoder.shouldMessageBeSkipped(read, lastReceivedLsn, startLsn, walPosition)) {
                    walQueue.add(new WalEntry(lastReceivedLsn, read));
                }

                if (!threadsRunning.get()) {
                    break;
                }
                read = stream.readPending();
            }
        }
        catch (SQLException e) {
            if (e.getCause() != null && e.getCause() instanceof SocketException) {
                LOGGER.info("Connection was lost while reading replication stream.", e);
                stopThreads();
            }
            else {
                LOGGER.warn("Could not read from replication stream on buffering thread.", e);
            }
        }
    }

}
