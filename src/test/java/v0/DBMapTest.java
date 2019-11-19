package v0;

import edu.yu.oats.oatsdb.dbms.*;
import edu.yu.oats.oatsdb.dbms.v0.DBMSImpl;
import edu.yu.oats.oatsdb.dbms.v0.TxMgrImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static edu.yu.oats.oatsdb.dbms.TxStatus.NO_TRANSACTION;
import static org.junit.Assert.fail;

public class DBMapTest {

    DBMSImpl myDb = DBMSImpl.Instance;
    TxMgrImpl transactionManeger = TxMgrImpl.Instance;

    @Test
    public void createMapPutThenGet(){
        int rtnVal = 0;
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.createMap("test map", String.class, Integer.class);
            testMap.put("testKey", 1);
            rtnVal = testMap.get("testKey");
            transactionManeger.commit();
        }
        catch (Exception e){
            //i sure hope it doesnt get here
        }
        Assert.assertSame(1, rtnVal);

    }

    @Test
    public void getMapTest(){
        int rtnVal = 0;
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.getMap("test map", String.class, Integer.class);
            //testMap.put("testKey", 1);
            rtnVal = testMap.get("testKey");
            transactionManeger.commit();
        }
        catch (Exception e){
            //i sure hope it doesnt get here
        }
        Assert.assertSame(1, rtnVal);
    }

    @Test
    public void createMapOutsideTransaction(){
        boolean notInTransactionThrown = false;
        try{
            myDb.createMap("This should fail", String.class, String.class);
        }
        catch (ClientNotInTxException e){
            notInTransactionThrown = true;
        }
        Assert.assertTrue(notInTransactionThrown);
    }

    @Test
    public void getMapButUseOutOfTransaction(){
        boolean notInTransactionThrown = false;
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.getMap("test map", String.class, Integer.class);
            //testMap.put("testKey", 1);
            try {
                final Tx tx = transactionManeger.getTx();
                if (tx.getStatus() != NO_TRANSACTION) {

                    final TxStatus txStatus = tx.getStatus();
                    if (txStatus == TxStatus.ACTIVE || txStatus == TxStatus.COMMITTING) {
                        System.out.println("Rolling back because it ");
                        transactionManeger.rollback();
                    }

                }
            }
            catch (Exception e) {
                fail("Was trying to end a transaction: failed ["+e+"]");
            }

            testMap.get("testKey");
        }
        catch (ClientNotInTxException e){
            notInTransactionThrown = true;
            System.out.println("in client not in exception");
        }
        catch (SystemException e){
            System.out.println("in System exception");
        }
        catch (NotSupportedException e){
            System.out.println("in NotSupportedException exception");
        }


        Assert.assertTrue(notInTransactionThrown);
    }


}
