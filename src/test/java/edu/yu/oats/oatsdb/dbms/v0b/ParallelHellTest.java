package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import java.util.Map;
import java.util.NoSuchElementException;

import static edu.yu.oats.oatsdb.dbms.TxStatus.NO_TRANSACTION;
import static org.junit.Assert.fail;

public class ParallelHellTest <K, V>{

    DBMSImpl myDb = edu.yu.oats.oatsdb.dbms.v0b.DBMSImpl.Instance;
    TxMgrImpl transactionManeger = edu.yu.oats.oatsdb.dbms.v0b.TxMgrImpl.Instance;
    private static Logger logger  = LogManager.getLogger();



    public static void main(String[] args) throws RollbackException, SystemException {
        try{
            ParallelHellTest test = new ParallelHellTest();
            test.myDb.setTxTimeoutInMillis(150);
            test.createMapPutThenGet();
            test.getMapTest();
            Thread.sleep(800);
            test.rolledBackWorkNotVisible();
            test.referenceUselessAfterCommit();
            test.referenceUselessAfterRollback();
            test.happyPathPutThenGet();
            test.happyPathPutThenGetThenModify();
            test.happyPathPutThenRemoveGetReturnsNull();
            test.happyPathPutThenRemovePutThenGet();
            test.happyPathPutThenRemoveThenGet();
            test.testRollbackOfCreateMap();

        //Multithreaded v0b/v0c tests

            test.testSimpleCompetingGets();
            Thread.sleep(800);
            test.testSimpleCompetingPuts();
            Thread.sleep(800);
            test.testAccountBalances();
            Thread.sleep(800);
        }
        catch (Exception e){
            logger.error("A thread exception thrown");
        }


    }

    public void createMapPutThenGet(){
        logger.info("######################  createMapPutThenGet  ######################");
        int rtnVal1 = 0;
        Person rtnVal2 = null;
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.createMap("test map", String.class, Integer.class);
            testMap.put("testKey", 1);
            rtnVal1 = testMap.get("testKey");
            Map<Integer, Person> personMap = myDb.createMap("person map", Integer.class, Person.class);
            personMap.put(1, new Person("Judah", 22, 2));
            personMap.put(2, new Person("MoRo", 22, 2));
            rtnVal2 = personMap.get(2);
            Map<String, BankAccount> accountMap = myDb.createMap("Accounts", String.class, BankAccount.class);
            BankAccount me = new BankAccount("Yehuda");
            accountMap.put("mine", me);
            accountMap.get("mine").addMoney(100);
            transactionManeger.commit();
        }
        catch (Exception e){
            e.printStackTrace();
            //i sure hope it doesnt get here
        }

        if(rtnVal1 == 1  && rtnVal2.getId() == 2){
            logger.info("Success");
        }
        else{
            logger.error("FAIL!");
        }
    }

    public void getMapTest(){
        logger.info("######################  getMapTest  ######################");
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                int rtnVal1 = 0;
                Person rtnVal2 = null;
                try {
                    transactionManeger.begin();
                    Map<String, Integer> testMap = myDb.getMap("test map", String.class, Integer.class);
                    rtnVal1 = testMap.get("testKey");
                    Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
                    rtnVal2 = personMap.get(2);
                    transactionManeger.commit();
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                if(rtnVal1 == 1 && rtnVal2.getName().equals("MoRo")){
                    logger.info("Success");
                }
                else{
                    logger.error("FAIL!");
                }
            }
        });
        t1.start();

    }

    public void createMapOutsideTransaction(){
        boolean notInTransactionThrown = false;
        try{
            myDb.createMap("This should fail", String.class, String.class);
        }
        catch (ClientNotInTxException e){
            notInTransactionThrown = true;
        }
        if(notInTransactionThrown){
            logger.info("Success");
        }
        else{
            logger.error("FAIL!");
        }    }

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

    public void rolledBackWorkNotVisible() {
        logger.info("######################  rolledBackWorkNotVisible  ######################");
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.getMap("test map", String.class, Integer.class);
            //testMap.put("testKey", 1);
            testMap.put("test rollback", 25);
            transactionManeger.rollback();

            transactionManeger.begin();
            testMap = myDb.getMap("test map", String.class, Integer.class);
            V rtnVal = (V)testMap.get("test rollback");
            transactionManeger.commit();

            if(rtnVal == null){
                logger.info("Success");
            }
            else{
                logger.error("FAIL!");
            }
        }
        catch (Exception e){
            fail("An exception was thrown and that shouldn't happen");
        }


    }

    public void referenceUselessAfterCommit() throws RollbackException, SystemException {
        logger.info("######################  referenceUselessAfterCommit  ######################");
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.getMap("test map", String.class, Integer.class);
            //testMap.put("testKey", 1);
            testMap.put("test rollback", 25);
            transactionManeger.commit();

            transactionManeger.begin();
            V rtnVal = (V)testMap.get("test rollback");
            transactionManeger.commit();

            if(rtnVal != null){
                logger.error("FAIL!");
            }
        }
        catch (ClientNotInTxException e){
            logger.info("Success that the client can't use the map");
            transactionManeger.commit();
        }
        catch (Exception e){
            logger.error("FAIL!");
        }

        try {
            transactionManeger.begin();
            Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
            personMap.put(25, new Person("Aliza", 23, 25));
            Person aliza = personMap.get(25);
            transactionManeger.commit();


            aliza.setName("Aliza my Wife");

            transactionManeger.begin();
            personMap = myDb.getMap("person map", Integer.class, Person.class);
            Person rtnVal = personMap.get(25);
            transactionManeger.commit();

            if(rtnVal.getName().equals("Aliza my Wife")){
                logger.error("FAIL!");
            }
            if(rtnVal.getName().equals("Aliza")){
                logger.info("Success, the client can't use the object after commit");
            }
        }
        catch (Exception e){
            logger.error("FAIL!");
        }
    }

    public void referenceUselessAfterRollback() throws RollbackException, SystemException {
        logger.info("######################  referenceUselessAfterRollback  ######################");
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.getMap("test map", String.class, Integer.class);
            //testMap.put("testKey", 1);
            testMap.put("test rollback", 25);
            transactionManeger.rollback();

            transactionManeger.begin();
            V rtnVal = (V)testMap.get("test rollback");
            transactionManeger.commit();

            if(rtnVal != null){
                logger.error("FAIL!");
            }
        }
        catch (ClientNotInTxException e){
            logger.info("Success");
            transactionManeger.commit();
        }
        catch (Exception e){
            logger.error("FAIL!");
        }

//        try {
//            transactionManeger.begin();
//            Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
//            personMap.put(18, new Person("Avi", 24, 18));
//            Person avi = personMap.get(18);
//            transactionManeger.rollback();
//
//            avi.setName("Avi my coach");
//
//            transactionManeger.begin();
//            personMap = myDb.getMap("person map", Integer.class, Person.class);
//            Person rtnVal = personMap.get(18);
//            transactionManeger.commit();
//
//            if(rtnVal.getName().equals("Avi my coach")){
//                logger.error("FAIL! was updated");
//            }
//            if(rtnVal.getName().equals("Avi")){
//                logger.error("Success, the client can't use the object after rollback");
//            }
//        }
//        catch (Exception e){
//            logger.error("FAIL! exception thrown");
//        }
    }

    public void happyPathPutThenGet() {
        logger.info("######################  happyPathPutThenGet  ######################");
        int rtnVal1 = 0;
        int rtnVal2 = 0;
        try {
            transactionManeger.begin();
            Map<String, Integer> testMap = myDb.getMap("test map", String.class, Integer.class);
            testMap.put("put then get test", 5);
            rtnVal1 = testMap.get("put then get test");
            transactionManeger.commit();

            transactionManeger.begin();
            testMap = myDb.getMap("test map", String.class, Integer.class);
            testMap.put("put then get test 2", 15);
            transactionManeger.commit();

            transactionManeger.begin();
            testMap = myDb.getMap("test map", String.class, Integer.class);
            rtnVal2 = testMap.get("put then get test 2");
            transactionManeger.commit();
        }
        catch (Exception e){
            fail("An exception was thrown and that shouldn't happen");
        }
        if(rtnVal1 == 5 && rtnVal2 == 15){
            logger.info("Success");
        }
        else{
            logger.error("FAIL!");
        }
    }

    public void happyPathPutThenGetThenModify() {
        logger.info("######################  happyPathPutThenGetThenModify  ######################");
        try{
            transactionManeger.begin();
            Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
            personMap.put(88, new Person("Avi Brick", 29, 88));
            Person aviBrick = personMap.get(88);
            aviBrick.setAge(30);
            transactionManeger.commit();

            transactionManeger.begin();
            personMap = myDb.getMap("person map", Integer.class, Person.class);
            aviBrick = personMap.get(88);
            transactionManeger.commit();

            if(aviBrick.getAge() != 30){
                logger.error("FAIL!");
            }
            if(aviBrick.getAge() == 30){
                logger.info("Success");
            }
        }
        catch (Exception e){
            logger.error(e.getStackTrace());
        }
    }

    public void happyPathPutThenRemoveThenGet() {
        logger.info("######################  happyPathPutThenRemoveThenGet  ######################");
        try{
            transactionManeger.begin();
            Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
            personMap.put(45, new Person("Eeema", 45, 45));
            Person eema = personMap.get(45);
            personMap.remove(45);
            transactionManeger.commit();

            transactionManeger.begin();
            personMap = myDb.getMap("person map", Integer.class, Person.class);
            eema = personMap.get(45);
            transactionManeger.commit();

            if(eema == null){
                logger.info("Success");
            }
            if(eema != null){
                logger.error("FAIL!");
            }
        }
        catch (Exception e){
            logger.error("Fail!");
            e.printStackTrace();
        }
    }

    public void happyPathPutThenRemovePutThenGet() {
        logger.info("######################  happyPathPutThenRemovePutThenGet  ######################");
        try{
            transactionManeger.begin();
            Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
            personMap.put(45, new Person("Eeema", 45, 45));
            Person eema = personMap.get(45);
            personMap.remove(45);
            personMap.put(45, new Person("Eeema second put", 55, 45));
            transactionManeger.commit();

            transactionManeger.begin();
            personMap = myDb.getMap("person map", Integer.class, Person.class);
            eema = personMap.get(45);
            transactionManeger.commit();

            if(eema.getName().equals("Eeema second put") && eema.getAge() == 55){
                logger.info("Success");
            }
            else{
                logger.error("FAIL!");
            }
        }
        catch (Exception e){
            logger.error("Fail!");
            e.printStackTrace();
        }
    }

    public void happyPathPutThenRemoveGetReturnsNull() {
        logger.info("######################  happyPathPutThenRemoveGetReturnsNull  ######################");
        try{
            transactionManeger.begin();
            Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
            personMap.put(45, new Person("Eeema", 45, 45));
            Person eema = personMap.get(45);
            personMap.remove(45);
            transactionManeger.commit();

            transactionManeger.begin();
            personMap = myDb.getMap("person map", Integer.class, Person.class);
            eema = personMap.get(45);
            transactionManeger.commit();

            if(eema == null){
                logger.info("Success");
            }
            if(eema != null){
                logger.error("FAIL!");
            }
        }
        catch (Exception e){
            logger.error("Fail!");
            e.printStackTrace();
        }
    }

    public void testRollbackOfCreateMap(){
        logger.info("######################  testRollbackOfCreateMap  ######################");
        Map<String, Integer> testMap;
        try {
            transactionManeger.begin();
            testMap = myDb.createMap("test create map", String.class, Integer.class);
            testMap.put("testKey", 1);
            transactionManeger.rollback();

            transactionManeger.begin();

            testMap = myDb.getMap("test create map", String.class, Integer.class);
            logger.info("the code reached this point");
            Integer rtn = testMap.get("testKey");
            if(rtn != null){
                logger.error("FAIL!");
            }
            transactionManeger.commit();
        }
        catch (NoSuchElementException e){
            logger.info("Success");
            try{
                transactionManeger.commit();
            }
            catch (Exception ex){
                logger.error("If it gets here were screwed what the heck?!");
            }
        } catch (RollbackException e) {
            logger.error("FAIL!");
            e.printStackTrace();
        } catch (SystemException e) {
            logger.error("FAIL!");
            e.printStackTrace();
        } catch (NotSupportedException e) {
            logger.error("FAIL!");
            e.printStackTrace();
        }

    }

    public void testSimpleCompetingGets(){
        logger.info("######################  testSimpleCompeting Gets  ######################");
        try{
            Thread t1 = new Thread(new FirstThreadSimpleGet());
            t1.start();
            Thread.sleep(5);
            Thread t2 = new Thread(new SecondThreadGet());
            t2.start();
        }
        catch (Exception e){

        }
    }

    public void testSimpleCompetingPuts(){
        logger.info("######################  testSimpleCompeting Puts  ######################");
        try{
            Thread t1 = new Thread(new FirstThreadPut());
            t1.start();
            Thread.sleep(5);
            Thread t2 = new Thread(new SecondThreadPut());
            t2.start();
            t1.join();
            t2.join();

            transactionManeger.begin();
            Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
            Person dad = personMap.get(5);
            transactionManeger.commit();
            if(dad.getName().equals("dad")){
                logger.info("Success");
            }
            else{
                logger.error("FAIL!");
            }
        }
        catch (Exception e){
            logger.error("FAIL! exception thrown!!");
            e.printStackTrace();
        }
    }

    public void testAccountBalances(){
        logger.info("######################  testAccountBalances  ######################");
        try{
            transactionManeger.begin();
            Map<String, BankAccount> accountMap = myDb.getMap("Accounts", String.class, BankAccount.class);
            BankAccount mine = accountMap.get("mine");
            logger.info("Before account test  " + mine.getAmount());
            transactionManeger.commit();
        }
        catch (Exception e){
            logger.error("Error thrown in the account balances section");
        }
        for(int i = 0; i < 5; i++){
            Thread t = new Thread(new AddMoney());
//            if(i % 2 == 0){
//            }
//            else {
//                t = new Thread(new WithdrawMoney());
//            }
            t.start();
            try{
                Thread.sleep(10);
            }
            catch (Exception e){
                logger.error("Thread error thrown");
            }
        }

        try{
            Thread.sleep(1000);
            transactionManeger.begin();
            Map<String, BankAccount> accountMap = myDb.getMap("Accounts", String.class, BankAccount.class);
            BankAccount mine = accountMap.get("mine");
            logger.info("After account test  " + mine.getAmount());
            transactionManeger.commit();
        }
        catch (Exception e){
            logger.error("Error thrown in the account balances section");
        }
    }

    public void valueClassMustBeSerializable() throws SystemException {
        throw new SystemException("");
    }

    //RUNNABLE CLASSES
    class FirstThreadSimpleGet implements Runnable{
        @Override
        public void run() {
            try{
                transactionManeger.begin();
                Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
                Person me = personMap.get(1);
                logger.info("First thread aquired judah person object");
                Thread.sleep(500);
                transactionManeger.commit();
            }
            catch (Exception e){
            }
        }
    }

    class SecondThreadGet implements Runnable{
        @Override
        public void run() {
            try{
                transactionManeger.begin();
                Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
                logger.info("Second thread attempting to get judah person object");
                Person me = personMap.get(1);
                transactionManeger.commit();
            }
            catch (ClientTxRolledBackException e){
                logger.info("ClientTxRolledBackException thrown on thread 2, SUCCESS");
            } catch (RollbackException e) {
                e.printStackTrace();
            } catch (SystemException e) {
                e.printStackTrace();
            } catch (NotSupportedException e) {
                e.printStackTrace();
            }
        }
    }

    class FirstThreadPut implements Runnable{
        @Override
        public void run() {
            try{
                transactionManeger.begin();
                Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
                personMap.put(5, new Person("dad", 60, 5));
                logger.info("First thread aquired dad person object lock");
                Thread.sleep(500);
                transactionManeger.commit();
            }
            catch (Exception e){
            }
        }
    }

    class SecondThreadPut implements Runnable{
        @Override
        public void run() {
            try{
                transactionManeger.begin();
                Map<Integer, Person> personMap = myDb.getMap("person map", Integer.class, Person.class);
                logger.info("Second thread attempting to get judah person object");
                personMap.put(5, new Person("dad?????", 0, 5));
                transactionManeger.commit();
            }
            catch (ClientTxRolledBackException e){
                logger.info("ClientTxRolledBackException thrown on thread 2, SUCCESS");
            } catch (RollbackException e) {
                e.printStackTrace();
            } catch (SystemException e) {
                e.printStackTrace();
            } catch (NotSupportedException e) {
                e.printStackTrace();
            }
        }
    }

    class AddMoney implements Runnable{
        @Override
        public void run() {
            try{
                transactionManeger.begin();
                Map<String, BankAccount> accountMap = myDb.getMap("Accounts", String.class, BankAccount.class);
                accountMap.get("mine").addMoney(10);
                transactionManeger.commit();
            } catch (Exception e) {
                logger.error("Tx timedout");
            }

        }
    }

    class WithdrawMoney implements Runnable{
        @Override
        public void run() {
            try{
                transactionManeger.begin();
                Map<String, BankAccount> accountMap = myDb.getMap("Accounts", String.class, BankAccount.class);
                accountMap.get("mine").withdrawMoney(5);
                transactionManeger.commit();
            }
            catch (Exception e){
                logger.error("Tx timedout");
            }
        }
    }

}

