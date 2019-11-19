package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public enum TxMgrImpl implements TxMgr {
    Instance;

    ThreadLocal<TxImpl> threadInfo = new ThreadLocal<TxImpl>();
    Logger logger  = LogManager.getLogger();

    public void begin() throws NotSupportedException, SystemException {
        if(threadInfo.get() != null){
            //logger.error("this thread already has a transaction");
            throw new NotSupportedException("We don't allow nested transactions");
        }
        else {
            TxImpl myTx = new TxImpl(TxStatus.ACTIVE);
            threadInfo.set(myTx);
            //logger.info("just started begin this is the status: " );
        }
    }

    public void commit() throws RollbackException, IllegalStateException, SystemException {
        if(threadInfo.get() == null){
            throw new IllegalStateException("You are not assosciated with a transaction at this time");
        }
        TxStatus currentStatus = this.threadInfo.get().getStatus();
        if(currentStatus == TxStatus.ROLLING_BACK || currentStatus == TxStatus.ROLLEDBACK){
            throw new RollbackException("This transaction was rolled back");
        }
        else if(currentStatus == TxStatus.ACTIVE){
            threadInfo.get().setStatus(TxStatus.COMMITTED);
            threadInfo.set(null);
        }
    }

    public void rollback() throws IllegalStateException, SystemException {
        // i feel like this is supposed to be done in the client code?
        // but it could og wither way with this one
        if(threadInfo.get() == null){
            throw new IllegalStateException("You are not assosciated with a transaction at this time");
        }
        else if (threadInfo.get().getStatus() == TxStatus.ACTIVE){
            threadInfo.get().setStatus(TxStatus.ROLLING_BACK);
            //do the actual rollback
            threadInfo.get().setStatus(TxStatus.ROLLEDBACK);
            threadInfo.set(null);
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
}
