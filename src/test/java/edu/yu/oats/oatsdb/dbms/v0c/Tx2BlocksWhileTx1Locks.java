package edu.yu.oats.oatsdb.dbms.v0c;

/** Verifies that if Tx2 must block if it accesses the same resource that is
 * currently locked by Tx1.  Tx1 relinquishes the lock in time to prevent Tx2
 * from rolling back.
 *
 * @author Avraham Leff
 */

import edu.yu.oats.oatsdb.dbms.ConfigurableDBMS;
import edu.yu.oats.oatsdb.dbms.OATSDBType;
import edu.yu.oats.oatsdb.dbms.TxMgr;
import edu.yu.oats.oatsdb.example.Account;
import edu.yu.oats.oatsdb.example.AccountFactory;
import edu.yu.oats.oatsdb.utils.RandomString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static edu.yu.oats.oatsdb.example.AccountType.SAVINGS;

public class Tx2BlocksWhileTx1Locks {

    private Tx2BlocksWhileTx1Locks() throws Exception {
        final RandomString randomString = new RandomString(6);
        _startTime = System.currentTimeMillis();
        _txRecords = new ArrayList<>();

        logger.info("Beginning 'setup tx'");
        txMgr.begin();
        _savings = dbms.createMap(savingAccounts, Long.class, Account.class);
        _account = AccountFactory.Instance.
                create(randomString.next(), 120, SAVINGS);
        _savings.put(_account.getAccountId(), _account);
        txMgr.commit();
        logger.info("Committed 'setup tx'");
    }

    private static class Tx1 implements Runnable {
        Tx1(final Tx2BlocksWhileTx1Locks outer) {
            Objects.requireNonNull(outer, "outer");
            _outer = outer;
            _id = "Tx1";
        }

        @Override
        public void run() {
            try {
                txMgr.begin();
                // read-only tx
                logger.debug("Retrieving account from map (in a tx)");
                final Account account = _outer._savings
                        .get(_outer._account.getAccountId());
                logger.debug("Tx {} retrieved account {}", _id, account);
                final long balance = account.getBalance();
                logger.debug("Tx {} account balance: {}", _id, balance);

                final int halfTimeout = halfTimeout();
                logger.debug("Tx {}: now sleeping for half tx timeout duration {}",
                        _id, halfTimeout);
                Thread.sleep(halfTimeout);
                logger.debug("Tx {}: waking up, and committing tx", _id);
                txMgr.commit();
            }
            catch(Exception e) {
                logger.error("Problem: ", e);
                throw new RuntimeException("Problem running Tx1", e);
            }

            _outer._txRecords.add(new TxRecord(_id));
            _outer._latch.countDown();
        }

        private final String _id;
        private final Tx2BlocksWhileTx1Locks _outer;
    }

    private static class Tx2 implements Runnable {
        Tx2(final Tx2BlocksWhileTx1Locks outer) {
            Objects.requireNonNull(outer, "outer");
            _outer = outer;
            _id = "Tx2";
        }

        @Override
        public void run() {
            try {
                // make sure that Tx1 gets in ahead of us and gets the lock
                Thread.sleep(10);

                txMgr.begin();
                // read-only tx
                logger.debug("Retrieving account from map (in a tx)");
                // supposed to block because Tx1 has the lock
                final Account account = _outer._savings
                        .get(_outer._account.getAccountId());
                final long accessTime = System.currentTimeMillis();
                logger.debug("Tx {} accessed account {} at {}",
                        _id, account, accessTime);
                _outer._txRecords.add(new TxRecord(_id));

                final long balance = account.getBalance();
                logger.debug("Tx {} account balance: {}", _id, balance);

                txMgr.commit();
            }
            catch(Exception e) {
                logger.error("Problem: ", e);
                throw new RuntimeException("Problem running Tx2", e);
            }

            _outer._latch.countDown();
        }

        private final String _id;
        private final Tx2BlocksWhileTx1Locks _outer;
    }


    private static class TxRecord {
        TxRecord(final String id) {
            Objects.requireNonNull(id, "id");
            _id = id;
            _timestamp = System.currentTimeMillis();
        }


        @Override
        public String toString() {
            return "TxRecord [id="+_id+", timestamp="+_timestamp+"]";
        }

        private final String _id;
        private final long _timestamp;
    }

    public static void main(final String[] args) throws Exception {
        logger.info("Starting, setting DBMS timeout to {}", timeout);
        dbms.setTxTimeoutInMillis(timeout);
        final Tx2BlocksWhileTx1Locks test = new Tx2BlocksWhileTx1Locks();
        logger.info("starting");
        final long startTime = System.currentTimeMillis();

        final Tx1 tx1 = new Tx1(test);
        final Tx2 tx2 = new Tx2(test);
        final Thread t1 = new Thread(tx1);
        t1.setName("tx1");
        final Thread t2 = new Thread(tx2);
        t2.setName("tx2");
        t1.start();
        t2.start();

        try {
            test._latch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        // NOTE: Tx2 SHOULD write its record AFTER Tx1 IF DBMS makes Tx2 block.
        // Otherwise, Tx1 will writes its wakeup record AFTER Tx2

        assert test._txRecords.size() == 2 : "should be exactly two records: " +
                test._txRecords;

        final TxRecord record1 = test._txRecords.get(0);
        assert record1._id == "Tx1" : "wrong record id: "+record1;
        assert isReasonableTimestamp(startTime + halfTimeout(), record1._timestamp)
                : "Wrong tx1 timestamp: "+record1;

        final TxRecord record2 = test._txRecords.get(1);
        assert record2._id == "Tx2" : "wrong record id: "+record2;
        // In v0b: Tx2 sleeps for an entire timeout period before waking up and
        // acquiring the lock on the resource that Tx1 released
        assert isReasonableTimestamp(startTime + (2 * halfTimeout())
                , record2._timestamp)
                : "Wrong tx2 timestamp: "+record2;

        logger.info("Exiting");
    }

    private static int halfTimeout() {
        return dbms.getTxTimeoutInMillis()/2;
    }

    private static boolean isReasonableTimestamp(long expected, long actual) {
        final long delta = Math.abs(expected - actual);
        if (delta > epsilonInMs) {
            logger.debug("Problem: expected={}, actual={}, absolute value diff={}",
                    expected, actual, delta);
            return false;
        }
        else {
            return true;
        }
    }


    // =========================================================================
    // ivars
    // =========================================================================
    private final static Logger logger =
            LogManager.getLogger(Tx2BlocksWhileTx1Locks.class);

    static ConfigurableDBMS dbms;
    static TxMgr txMgr;
    static {
        try {
            dbms = (ConfigurableDBMS) OATSDBType.dbmsFactory(OATSDBType.V0c);
            txMgr = OATSDBType.txMgrFactory(OATSDBType.V0c);
        }
        catch (InstantiationException e) {
            throw new RuntimeException("Couldn't initialize Singletons", e);
        }
    }

    final static int epsilonInMs = 50;
    final static String savingAccounts = "SavingAccounts";
    final static int timeout = 5 * 1000; // 5 seconds in ms

    private final long _startTime;
    private final List<TxRecord> _txRecords;
    private final CountDownLatch _latch = new CountDownLatch(2);
    private final Map<Long, Account> _savings;
    private final Account _account;
}
