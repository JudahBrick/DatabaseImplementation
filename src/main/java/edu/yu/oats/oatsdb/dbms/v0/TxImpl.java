package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.SystemException;
import edu.yu.oats.oatsdb.dbms.Tx;
import edu.yu.oats.oatsdb.dbms.TxCompletionStatus;
import edu.yu.oats.oatsdb.dbms.TxStatus;

public class TxImpl implements Tx {

    private TxStatus status;

    protected TxImpl(TxStatus status){
        this.status = status;
    }

    protected void setStatus(TxStatus status){
        this.status = status;
    }

    public TxStatus getStatus() throws SystemException {
        return status;
    }

    public TxCompletionStatus getCompletionStatus() {
        switch(status){
            case COMMITTED:
                return TxCompletionStatus.COMMITTED;
            case ROLLEDBACK:
                return TxCompletionStatus.ROLLEDBACK;
            default:
                return TxCompletionStatus.NOT_COMPLETED;
        }
    }
}
