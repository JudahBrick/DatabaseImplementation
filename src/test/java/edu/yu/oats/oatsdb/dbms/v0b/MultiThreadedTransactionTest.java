package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MultiThreadedTransactionTest {

    TxMgr myTxMan;

    MultiThreadedTransactionTest(){
        myTxMan = edu.yu.oats.oatsdb.dbms.v0b.TxMgrImpl.Instance;
    }

    private static Logger logger  = LogManager.getLogger();

    public static void main(String[] arg) {
        try {
            logger.info("Starting Multithreaded test");
            MultiThreadedTransactionTest test = new MultiThreadedTransactionTest();
            test.happyPath(20);
            Thread.sleep(500);
            test.beginTxCantBeInsideATx(20);
            Thread.sleep(500);
            test.testTxRollback(20);
            Thread.sleep(500);
            test.testTxCommit();
            Thread.sleep(500);
            test.committedTxNoLongerInTx(20);
            Thread.sleep(500);
            test.rolledbackTxNoLongerInTx();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void happyPath(int numOfThreads){
        //ThreadGroup threadGroup = new ThreadGroup();
        logger.info("happy path test start");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        boolean firstBegin = false;
                        boolean secondBegin = false;
                        boolean firstCommit = false;
                        boolean secondCommit = false;
                        myTxMan.begin();
                        Tx myTx = myTxMan.getTx();
                        if(myTx.getStatus() == TxStatus.ACTIVE){
                            firstBegin = true;
                        }
                        myTxMan.commit();
                        if(myTx.getStatus() == TxStatus.COMMITTED){
                            firstCommit = true;
                        }
                        myTxMan.begin();
                        myTx = myTxMan.getTx();
                        if(myTx.getStatus() == TxStatus.ACTIVE){
                            secondBegin = true;
                        }
                        myTxMan.commit();
                        if(myTx.getStatus() == TxStatus.COMMITTED){
                            secondCommit = true;
                        }
                        if(firstBegin && firstCommit && secondBegin && secondCommit){
                            logger.info("SUCCESS");
                        }
                        else {
                            logger.error("FAIL");
                        }
                    } catch (NotSupportedException e) {
                        logger.error("FAIL");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    } catch (RollbackException e) {
                        logger.error("FAIL");
                    }
                }
            });
            testThread.start();
        }
    }

    private void beginTxCantBeInsideATx(int numOfThreads){
        logger.info("beginTxCantBeInsideATx test started");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        myTxMan.begin();
                        myTxMan.begin();
                    } catch (NotSupportedException e) {
                        logger.info("Test was successful");
                    } catch (SystemException e) {
                        logger.error("FAIL");
                    }
                }
            });
            testThread.start();
        }
    }

    private void testTxRollback(int numOfThreads){
        logger.info("testTxRollback test started");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        boolean begin = false;
                        boolean rolledBack = false;
                        myTxMan.begin();
                        Tx myTx = myTxMan.getTx();
                        if(myTx.getStatus() == TxStatus.ACTIVE && myTxMan.getStatus() == TxStatus.ACTIVE){
                            begin = true;
                        }
                        myTxMan.rollback();
                        if(myTx.getStatus() == TxStatus.ROLLEDBACK && myTxMan.getStatus() == TxStatus.NO_TRANSACTION){
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
            });
            testThread.start();
        }
    }

    private void testTxCommit(){
        logger.info("testTxCommit test started");
        for(int i = 0; i < 20; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        boolean begin = false;
                        boolean committed = false;
                        myTxMan.begin();
                        Tx myTx = myTxMan.getTx();
                        if(myTx.getStatus() == TxStatus.ACTIVE && myTxMan.getStatus() == TxStatus.ACTIVE){
                            begin = true;
                        }
                        myTxMan.commit();
                        if(myTx.getStatus() == TxStatus.COMMITTED && myTxMan.getStatus() == TxStatus.NO_TRANSACTION){
                            committed = true;
                        }
                        if(committed && begin){
                            logger.info("SUCCESS");
                        }
                        else{
                            logger.error("FAIL: begin: " + begin + " Committed: " + committed);
                        }

                    } catch (NotSupportedException e) {
                        logger.error("FAIL  NotSupportedException thrown");
                    } catch (SystemException e) {
                        logger.error("FAIL  SystemException thrown");
                    } catch (RollbackException e) {
                        logger.error("FAIL  RollbackException thrown");
                    }
                }
            });
            testThread.start();
        }
    }

    private void committedTxNoLongerInTx(int numOfThreads){
        logger.info("committedTxNoLongerInTx test started");
        for(int i = 0; i < numOfThreads; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    boolean commitednoLongerInTX = false;
                    try {

                        myTxMan.begin();
                        myTxMan.commit();
                        if(myTxMan.getStatus() == TxStatus.NO_TRANSACTION){
                            commitednoLongerInTX =true;
                        }
                        myTxMan.commit();
                        logger.error("FAIL: An exception was supposed to thrown");
                    } catch (IllegalStateException e) {
                        if(commitednoLongerInTX){
                            logger.info("SUCCESS");
                        }
                        else{
                            logger.error("FAIL: exception thrown but thread still has a transaction attached to it");
                        }
                    } catch (SystemException e) {
                        logger.error("FAIL  SystemException thrown");
                    } catch (RollbackException e) {
                        logger.error("FAIL  RollbackException thrown");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL  NotSupportedException thrown");
                    }
                }
            });
            testThread.start();
        }
    }

    private void rolledbackTxNoLongerInTx(){
        logger.info("rolledbackTxNoLongerInTx test started");
        for(int i = 0; i < 20; i++){
            Thread testThread = new Thread(new Runnable() {
                public void run() {
                    boolean rolledbackTxNoLongerInTx = false;
                    try {

                        myTxMan.begin();
                        myTxMan.commit();
                        if(myTxMan.getStatus() == TxStatus.NO_TRANSACTION){
                            rolledbackTxNoLongerInTx =true;
                        }
                        myTxMan.commit();
                        logger.error("FAIL: An exception was supposed to thrown");
                    } catch (IllegalStateException e) {
                        if(rolledbackTxNoLongerInTx){
                            logger.info("SUCCESS");
                        }
                        else{
                            logger.error("FAIL: exception thrown but thread still has a transaction attached to it");
                        }
                    } catch (SystemException e) {
                        logger.error("FAIL  SystemException thrown");
                    } catch (RollbackException e) {
                        logger.error("FAIL  RollbackException thrown");
                    } catch (NotSupportedException e) {
                        logger.error("FAIL  NotSupportedException thrown");
                    }
                }
            });
            testThread.start();
        }
    }
}
