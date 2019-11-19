package edu.yu.oats.oatsdb.dbms.v0b;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class DBMSImplTest {
    edu.yu.oats.oatsdb.dbms.v0b.DBMSImpl myDb = edu.yu.oats.oatsdb.dbms.v0b.DBMSImpl.Instance;
    edu.yu.oats.oatsdb.dbms.v0b.TxMgrImpl transactionManeger = edu.yu.oats.oatsdb.dbms.v0b.TxMgrImpl.Instance;


    @Test
    public void createMap() {
        int rtnVal = 0;
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.createMap("test map", String.class, Integer.class);
            Map<String, Integer> testMap2 = myDb.createMap("test map2", String.class, Integer.class);
            testMap.put("testKey", 1);
            rtnVal = testMap.get("testKey");
            transactionManeger.commit();
        }
        catch (Exception e){
            //i sure hope it doesnt get here
        }
        Assert.assertSame(1, rtnVal);
    }

}