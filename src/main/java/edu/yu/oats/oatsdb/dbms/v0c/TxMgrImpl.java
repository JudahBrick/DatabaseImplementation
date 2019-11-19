package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum TxMgrImpl implements TxMgr {
    Instance;

    ThreadLocal<TxImpl> threadInfo = new ThreadLocal<TxImpl>();
    ThreadLocal<Boolean> rolledBack = new ThreadLocal<>();
    Logger logger  = LogManager.getLogger();

    public void begin() throws NotSupportedException, SystemException {
        logger.debug("           BEGIN    "  + Thread.currentThread().getName());
        if(threadInfo.get() != null){
            throw new NotSupportedException("We don't allow nested transactions");
        }
        else {
            TxImpl myTx = new TxImpl(TxStatus.ACTIVE);
            threadInfo.set(myTx);
            rolledBack.set(false);
            //DBMSImpl.Instance.setUpShadow();
            DBMSImpl.Instance.setMapLocksForTransaction(myTx);
        }
    }

    public void commit() throws RollbackException, IllegalStateException, SystemException {
        logger.debug("           COMMIT    "  + Thread.currentThread().getName());
        if(rolledBack.get()){
            throw new RollbackException("This transaction was rolled back");
        }

        if(threadInfo.get() == null){
            throw new IllegalStateException("You are not assosciated with a transaction at this time");
        }
        TxStatus currentStatus = this.threadInfo.get().getStatus();
        if(currentStatus == TxStatus.ACTIVE){
            threadInfo.get().setStatus(TxStatus.COMMITTING);
            threadInfo.get().commit();
            threadInfo.get().setStatus(TxStatus.COMMITTED);
            threadInfo.set(null);
        }
    }

    public void rollback() throws IllegalStateException, SystemException {
        logger.debug("           ROLLBACK    "  + Thread.currentThread().getName());
        if(threadInfo.get() == null){
            throw new IllegalStateException("You are not assosciated with a transaction at this time");
        }
        else if (threadInfo.get().getStatus() == TxStatus.ACTIVE || threadInfo.get().getStatus() == TxStatus.COMMITTING){
            threadInfo.get().setStatus(TxStatus.ROLLING_BACK);
            threadInfo.get().rollback();
            threadInfo.get().setStatus(TxStatus.ROLLEDBACK);
            threadInfo.set(null);
            rolledBack.set(true);
        }
    }

    public Tx getTx() throws SystemException {
        if(threadInfo.get() == null){
            return new TxImpl(TxStatus.NO_TRANSACTION);
        }
        return threadInfo.get();

    }

    public TxStatus getStatus() throws SystemException {
        if(threadInfo.get() == null){
            return TxStatus.NO_TRANSACTION;
        }
        return threadInfo.get().getStatus();
    }

//    protected UUID getTxId(){
//        if(threadInfo.get() != null){
//            return threadInfo.get().getId();
//        }
//        return null;
//    }
}