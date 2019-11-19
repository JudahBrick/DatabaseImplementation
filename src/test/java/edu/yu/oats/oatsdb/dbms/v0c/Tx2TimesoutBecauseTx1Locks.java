package edu.yu.oats.oatsdb.dbms.v0c;

/** Verifies that, because Tx2 blocks if it accesses the same resource that is
 * currently locked by Tx1, it will timeout and get rolled back by the DBMS.
 * The DBMS currently allows clients to sleep once, attempt to acquire the lock
 * & fail, and only then rollback the tx.  So, Tx2 will be rolled back only
 * after TWO timeout periods have passed.
 *
 * @author Avraham Leff
 */

import edu.yu.oats.oatsdb.dbms.ClientTxRolledBackException;
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

public class Tx2TimesoutBecauseTx1Locks {

    private Tx2TimesoutBecauseTx1Locks() throws Exception {
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
        Tx1(final Tx2TimesoutBecauseTx1Locks outer) {
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

                final Account account = _outer._savings.get(_outer._account.getAccountId());
                logger.debug("Tx {} retrieved account {}", _id, account);
                final long balance = account.getBalance();
                logger.debug("Tx {} account balance: {}", _id, balance);

                // ensure that Tx2 will not be able to acquire the lock even after
                // sleeping for ONE timeout: i.e., will wake up once, and STILL not be
                // able to acquire the lock.  This will cause the DBMS to abort the tx.
                final int moreThanTimeout = moreThanTimeout();
                logger.debug("Tx {}: now sleeping for 'more than' tx timeout duration {}",
                        _id, moreThanTimeout);
                Thread.sleep(moreThanTimeout);
                logger.debug("Tx {}: waking up, and committing tx", _id);
                // IMPORTANT: The DBMS will NOT rollback Tx1 despite the fact that it's
                // sleeping for more than the timeout!  In our implementation, DBMS
                // only sets ITS timeout if a Tx accesses a transactional resource.
                // Tx1 here ALREADY HAS THE RESOURCE, so as far as the DBMS is
                // concerned, it can keep on sleeping forever.
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
        private final Tx2TimesoutBecauseTx1Locks _outer;
    }

    private static class Tx2 implements Runnable {
        Tx2(final Tx2TimesoutBecauseTx1Locks outer) {
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
                logger.debug("Tx {} attemps to retrieve account from map (in a tx)",
                        _id);
                _outer._txRecords.add(new TxRecord(_id));
                _outer._latch.countDown();
                // supposed to block because Tx1 has the lock
                final Account account = _outer._savings.get(_outer._account.getAccountId());
                // supposed to be timed out and the get() operation above will receive
                // an exception after being rolledback
                if (true) {
                    logger.error("Tx {} should not have arrived here", _id);
                    throw new IllegalStateException("Tx should have caused get() to "+
                            "throw ClientTxRolledBackException");
                }
                txMgr.commit();
            }
            catch(Exception e) {
                if (e instanceof ClientTxRolledBackException) {
                    logger.info("Expected occurred: tx {} rolled back", _id);
                    final long accessTime = System.currentTimeMillis();
                    logger.debug("Tx {} rolled back at {}", _id, accessTime);
                    _outer._txRecords.add(new TxRecord(_id));
                    _outer._latch.countDown();
                }
                else {
                    logger.error("Problem: ", e);
                    throw new RuntimeException("Problem running Tx2", e);
                }
            } // exception
        }   // run

        private final String _id;
        private final Tx2TimesoutBecauseTx1Locks _outer;
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
        final Tx2TimesoutBecauseTx1Locks test = new Tx2TimesoutBecauseTx1Locks();
        System.out.println("starting");
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

        assert test._txRecords.size() == 3 : "should be exactly three records: " +
                test._txRecords;

        final TxRecord record1 = test._txRecords.get(0);
        assert record1._id == "Tx2" : "wrong record id: "+record1;
        // This should be tx2 trying to access the record that tx1 locked
        assert isReasonableTimestamp(startTime, record1._timestamp)
                : "Wrong tx2 timestamp: "+record1;
        logger.info("record1 {} ok", record1);

        final TxRecord record2 = test._txRecords.get(1);
        assert record2._id == "Tx1" : "wrong record id: "+record2;
        // This should be the tx1 having woken up and committed
        assert isReasonableTimestamp(startTime + moreThanTimeout(),
                record2._timestamp)
                : "Wrong tx1 timestamp: "+record2;
        logger.info("record2 {} ok", record2);

        final TxRecord record3 = test._txRecords.get(2);
        assert record3._id == "Tx2" : "wrong record id: "+record3;
        // should be tx2 timing out and being automagically rolled back
        assert isReasonableTimestamp(startTime + (2 * dbms.getTxTimeoutInMillis()),
                record3._timestamp)
                : "Wrong tx2 timestamp on being rolled back: "+record3;

        logger.info("record3 {} ok", record3);

        System.out.println("finishing");
        logger.info("Exiting");
    }

    private static int moreThanTimeout() { return dbms.getTxTimeoutInMillis() + 50; }


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
            LogManager.getLogger(Tx2TimesoutBecauseTx1Locks.class);

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
    private final CountDownLatch _latch = new CountDownLatch(3); // NOTE: "3"
    private final Map<Long, Account> _savings;
    private final Account _account;
}

