package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TxMgrImplTest {
    edu.yu.oats.oatsdb.dbms.v0b.TxMgrImpl myTestManager = edu.yu.oats.oatsdb.dbms.v0b.TxMgrImpl.Instance;
    private static Logger logger  = LogManager.getLogger();

    @Test
    public  void noTransactionBeforeBegin() throws SystemException {
        Assert.assertSame(TxStatus.NO_TRANSACTION, myTestManager.getStatus());
    }

    @Test
    public void testHappyTrans() throws SystemException, NotSupportedException, RollbackException {
        boolean begin = false;
        boolean commited = false;
        boolean both = false;
        myTestManager.begin();
        if(myTestManager.getStatus() == TxStatus.ACTIVE){
            begin = true;
        }
        Tx myTx = myTestManager.getTx();
        myTestManager.commit();
        if(myTestManager.getStatus() == TxStatus.NO_TRANSACTION){
            commited = true;
        }
        if(myTx.getStatus() != TxStatus.COMMITTED){
            commited = false;
        }
        if(begin && commited){
            both = true;
        }
        Assert.assertTrue(both);
        System.out.println(myTestManager.getTx().getStatus());

    }

    @Test
    public void testHappyRollback() throws SystemException, NotSupportedException, RollbackException {
        try {
            boolean begin = false;
            boolean rolledBack = false;
            myTestManager.begin();
            Tx myTx = myTestManager.getTx();
            if(myTx.getStatus() == TxStatus.ACTIVE && myTestManager.getStatus() == TxStatus.ACTIVE){
                begin = true;
            }
            myTestManager.rollback();
            if(myTx.getStatus() == TxStatus.ROLLEDBACK && myTestManager.getStatus() == TxStatus.NO_TRANSACTION){
                rolledBack = true;
            }
            if(rolledBack && begin){
                logger.info("SUCCESS");
            }
            else{
                logger.error("FAIL: begin: " + begin + " Rolled back: " + rolledBack);
            }

        } catch (NotSupportedException e) {
            logger.error("FAIL  NotSupportedException thrown");
        } catch (SystemException e) {
            logger.error("FAIL  SystemException thrown");
        }

    }

    @Test
    public void commitNoBegin() throws SystemException, NotSupportedException, RollbackException{
        boolean illegalStateThrown = false;
        try{
            myTestManager.begin();
            myTestManager.commit();
            myTestManager.commit();
        }
        catch (IllegalStateException e){
            illegalStateThrown = true;
        }

        Assert.assertTrue(illegalStateThrown);
    }
}