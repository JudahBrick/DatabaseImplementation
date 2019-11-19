package v0;

import edu.yu.oats.oatsdb.dbms.*;
import edu.yu.oats.oatsdb.dbms.v0.TxMgrImpl;
import org.junit.Assert;
import org.junit.Test;

public class TxMgrImplTest {

    TxMgrImpl myTestManeger = TxMgrImpl.Instance;

    @Test
    public  void noTransactionBeforeBegin() throws SystemException {
        Assert.assertSame(TxStatus.NO_TRANSACTION, myTestManeger.getStatus());
    }

    @Test
    public void testHappyTrans() throws SystemException, NotSupportedException, RollbackException {
        boolean begin = false;
        boolean commited = false;
        boolean both = false;
        myTestManeger.begin();
        if(myTestManeger.getStatus() == TxStatus.ACTIVE){
            begin = true;
        }
        Tx myTx = myTestManeger.getTx();
        myTestManeger.commit();
        if(myTestManeger.getStatus() == TxStatus.NO_TRANSACTION){
            commited = true;
        }
        if(myTx.getStatus() != TxStatus.COMMITTED){
            commited = false;
        }
        if(begin && commited){
            both = true;
        }
        Assert.assertTrue(both);
        System.out.println(myTestManeger.getTx().getStatus());

    }

    @Test
    public void testHappyRollback() throws SystemException, NotSupportedException, RollbackException {

        myTestManeger.begin();
        Assert.assertSame(TxStatus.ACTIVE, myTestManeger.getStatus());

        myTestManeger.rollback();
        Assert.assertSame(TxStatus.ROLLEDBACK, myTestManeger.getStatus());

    }

    @Test
    public void commitNoBegin() throws SystemException, NotSupportedException, RollbackException{
        boolean illegalStateThrown = false;
        try{
            myTestManeger.begin();
            myTestManeger.commit();
            myTestManeger.commit();
        }
        catch (IllegalStateException e){
            illegalStateThrown = true;
        }

        Assert.assertTrue(illegalStateThrown);
    }
}
