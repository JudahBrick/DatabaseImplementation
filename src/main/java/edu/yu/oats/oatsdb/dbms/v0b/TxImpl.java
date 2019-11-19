package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.SystemException;
import edu.yu.oats.oatsdb.dbms.Tx;
import edu.yu.oats.oatsdb.dbms.TxCompletionStatus;
import edu.yu.oats.oatsdb.dbms.TxStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
//import java.util.UUID;

public class TxImpl implements Tx {

    private TxStatus status;
    //private UUID ID;
    private ArrayList<MapEntryLock> realMaps;
    Logger logger  = LogManager.getLogger();

    protected TxImpl(TxStatus status){
        this.status = status;
        //ID = UUID.randomUUID();
        realMaps = new ArrayList<>();
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

    protected void commit() throws SystemException {
        logger.debug("        COMMIT IN TX   "  + Thread.currentThread().getName());
        logger.debug("      Number of mapLocks: " + realMaps.size());
        for(MapEntryLock mapLock: realMaps){
            mapLock.commit();
        }
    }

    protected void rollback(){
        logger.debug("        ROLLBACK IN TX   "  + Thread.currentThread().getName());
        logger.debug("      Number of mapLocks: " + realMaps.size());

        for(MapEntryLock mapLock: realMaps){
            mapLock.rollback();
        }
    }

    protected void addMapEntryLock(MapEntryLock mapLock){
        logger.debug("Map Lock added" );
        realMaps.add(mapLock);
    }

//    protected UUID getId(){
//        return ID;
//    }

}
