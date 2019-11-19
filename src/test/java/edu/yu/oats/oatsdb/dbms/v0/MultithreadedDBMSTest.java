package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.NoSuchElementException;

public class MultithreadedDBMSTest {

    DBMS myDBMS = DBMSImpl.Instance;
    TxMgr myTxMng = TxMgrImpl.Instance;
    private static Logger logger  = LogManager.getLogger();

    public static void main(String[] args){
        try{
            MultithreadedDBMSTest test = new MultithreadedDBMSTest();
            test.vanilla();
            Thread.sleep(500);
            test.vanillaMapsTest();
            Thread.sleep(500);
            test.insertIntoMapMustBeInTx();
            Thread.sleep(500);
            test.getMapMustBeInTx();
            Thread.sleep(500);
            test.retrieveNameCannotBeEmpty();
            Thread.sleep(500);
            test.createNameCannotBeEmpty();
            Thread.sleep(500);
            test.createMapMustBeInTx();
            Thread.sleep(500);
            test.cannotCreateIfNameIsAlreadyInUse();
            Thread.sleep(500);
            test.retrieveOnlyIfCreated();
            Thread.sleep(500);
            test.typesOfValueClassEnforced();
            Thread.sleep(500);
            test.typesOfKeyClassEnforced();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void vanilla(){
        logger.info("Vanilla test start");
        try{
            myTxMng.begin();
            Map<String, String> firstTestMap = myDBMS.createMap("first test map", String.class, String.class);
            Map<String, String> secondTestMap = myDBMS.createMap("second test map", String.class, String.class);
            Map<String, String> thirdTestMap = myDBMS.createMap("third test map", String.class, String.class);
            firstTestMap.put("firsttestkey", "firstTestVal");
            secondTestMap.put("one", "1");
            thirdTestMap.put("third", "3");
            myTxMng.commit();
            logger.info("SUCCESS");
        }catch (Exception e){
            logger.error("FAIL");
        }
    }

    private void vanillaMapsTest(){
        logger.info("Vanilla map test start");
        boolean isThere = false;
        try {
            myTxMng.begin();
            Map<String, String> mapFun = myDBMS.getMap("first test map", String.class, String.class);
            mapFun.put("test key for map", "test val for map");
            isThere = mapFun.containsKey("test key for map");
            myTxMng.commit();
        } catch (NotSupportedException e) {
            logger.error("FAIL");
        } catch (SystemException e) {
            logger.error("FAIL");
        } catch (RollbackException e) {
            logger.error("FAIL");
        }
        if(isThere){
            logger.info("SUCCESS");
        }
        else {
            logger.error("FAIL");
        }
    }

    private void insertIntoMapMustBeInTx(){
        final int numOfThreads = 20;
        logger.info("insertIntoMapMustBeInTx test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        //myTxMng.begin();
                        String name = "test map" + numOfThreads;
                        myDBMS.createMap(name, String.class, Integer.class);
                        //myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClientNotInTxException e){
                        logger.info("SUCCESS");
                    }
                }
            });
            testThread.start();
        }
    }

    private void getMapMustBeInTx(){
        final int numOfThreads = 20;
        logger.info("getMapMustBeInTx test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        //myTxMng.begin();
                        String name = "test map" + numOfThreads;
                        myDBMS.getMap(name, String.class, Integer.class);
                        //myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClientNotInTxException e){
                        logger.info("SUCCESS");
                    }
                }
            });
            testThread.start();
        }
    }

    private void retrieveNameCannotBeEmpty(){
        final int numOfThreads = 20;
        logger.info("retrieveNameCannotBeEmpty test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMng.begin();
                        myDBMS.getMap("", String.class, Integer.class);
                        myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClientNotInTxException e){
                        logger.error("FAIL");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    } catch (NoSuchElementException e) {
                        logger.error("FAIL");
                    }catch (IllegalArgumentException e){
                        logger.info("SUCCESS");
                    }
                }
            });
            testThread.start();
        }
    }

    private void createMapMustBeInTx(){
        final int numOfThreads = 20;
        logger.info("createMapMustBeInTx test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMng.begin();
                        myDBMS.createMap("", String.class, Integer.class);
                        myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClientNotInTxException e){
                        logger.error("FAIL");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    } catch (NoSuchElementException e) {
                        logger.error("FAIL");
                    } catch (IllegalArgumentException e){
                        logger.info("SUCCESS");
                    }
                }
            });
            testThread.start();
        }
    }

    private void cannotCreateIfNameIsAlreadyInUse(){
        final int numOfThreads = 20;
        logger.info("cannotCreateIfNameIsAlreadyInUse test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMng.begin();
                        myDBMS.createMap("hi", String.class, Integer.class);
                        myDBMS.createMap("hi", String.class, Integer.class);
                        myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClientNotInTxException e){
                        logger.error("FAIL");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    } catch (NoSuchElementException e) {
                        logger.error("FAIL");
                    } catch (IllegalArgumentException e){
                        logger.info("SUCCESS");
                    }
                }
            });
            testThread.start();
        }
    }

    private void retrieveOnlyIfCreated(){
        final int numOfThreads = 20;
        logger.info("retrieveOnlyIfCreated test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMng.begin();
                        myDBMS.getMap("this map name does not exist", String.class, Integer.class);
                        myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClientNotInTxException e){
                        logger.error("FAIL");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    } catch (NoSuchElementException e) {
                        logger.info("SUCCESS");
                    } catch (IllegalArgumentException e){
                        logger.error("FAIL");
                    }
                }
            });
            testThread.start();
        }
    }

    private void typesOfValueClassEnforced(){
        final int numOfThreads = 20;
        logger.info("typesOfValueClassEnforced test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMng.begin();
                        myDBMS.getMap("first test map", String.class, Integer.class);
                        myTxMng.commit();
                        logger.error("FAIL");
                    }
                     catch (ClassCastException e){
                        logger.info("SUCCESS");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    }
                }
            });
            testThread.start();
        }
    }

    private void typesOfKeyClassEnforced(){
        final int numOfThreads = 20;
        logger.info("typesOfKeyClassEnforced test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMng.begin();
                        myDBMS.getMap("second test map", Integer.class, String.class);
                        myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClassCastException e){
                        logger.info("SUCCESS");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    }
                }
            });
            testThread.start();
        }
    }

    private void createNameCannotBeEmpty(){
        final int numOfThreads = 20;
        logger.info("createNameCannotBeEmpty test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMng.begin();
                        myDBMS.createMap("    ", String.class, Integer.class);
                        myTxMng.commit();
                        logger.error("FAIL");
                    }
                    catch (ClientNotInTxException e){
                        logger.error("FAIL");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    } catch (NoSuchElementException e) {
                        logger.error("FAIL");
                    }catch (IllegalArgumentException e){
                        logger.info("SUCCESS");
                    }
                }
            });
            testThread.start();
        }
    }

}


//edu.yu.oats.oatsdb.test.v0.TestDBMS  - 11 tests
//        retrieveFromMapMustBeInTx
//
//        edu.yu.oats.oatsdb.test.v0.TestTx  - 7 tests
//        testTxCommit
//        testHappyPath
//        beginTxCantBeInsideATx
//        testTxRollback
//        rolledbackTxNoLongerInTx
//        threadMustBeInATx
//        committedTxNoLongerInTx