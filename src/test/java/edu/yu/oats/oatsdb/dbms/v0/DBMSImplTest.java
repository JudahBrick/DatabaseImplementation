package edu.yu.oats.oatsdb.dbms.v0;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class DBMSImplTest {

    DBMSImpl myDb = DBMSImpl.Instance;
    TxMgrImpl transactionManeger = TxMgrImpl.Instance;


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

//    @Test
//    public void getMap() {
//        int rtnVal = 0;
//        try {
//            transactionManeger.begin();
//            myDb.createMap("test map for get", String.class, Integer.class);
//            Map<String, Integer> testMap = myDb.getMap("test map for get", String.class, Integer.class);
//            //testMap.put("testKey", 1);
//            rtnVal = testMap.get("testKey");
//            transactionManeger.commit();
//        }
//        catch (Exception e){
//            //i sure hope it doesnt get here
//        }
//        Assert.assertSame(1, rtnVal);
//    }
}