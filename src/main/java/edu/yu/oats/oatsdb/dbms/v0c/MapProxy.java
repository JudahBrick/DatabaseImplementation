package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.ClientNotInTxException;
import edu.yu.oats.oatsdb.dbms.ClientTxRolledBackException;
import edu.yu.oats.oatsdb.dbms.SystemException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

//, V extends Serializable taken out ot class signature
public class MapProxy<K, V> implements Map<K, V> {

    private Map<K, V> map;
    private ThreadLocal<ConcurrentHashMap<K, V>> shadowMap = new ThreadLocal<>();
    private Map<K, ReentrantLock> locks;
    //private Map<K, Condition> conditionVars;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private int timeoutInMilli = 0;
    private ThreadLocal<MapEntryLock> mapLocks = new ThreadLocal<>();
    //private UUID txID;
    private boolean createMapCall;
    private String name;
    private static Logger logger  = LogManager.getLogger();


    //TODO read the lines below and do them
    // need to have a transaction associated with a shadow db
    // so that the shadow db can check to make sure its the same transaction that original used this shadow db
    // maybe its better if on commit i can just change the status of the shadow db
    // so it is basically no op or unusable


    protected MapProxy(Class<K> class1, Class<V> class2, String name){
        keyClass = class1;
        valueClass = class2;
        map =  new ConcurrentHashMap<>();
        locks = new ConcurrentHashMap<>();
        //conditionVars = new ConcurrentHashMap<>();
        //txID = TxMgrImpl.Instance.getTxId();
        createMapCall = true;
        this.name = name;
    }

    protected void setUpNewThread(TxImpl tx){
        if(shadowMap.get() == null){
            shadowMap.set(new ConcurrentHashMap<K, V>());
        }
        if(mapLocks.get() == null){
            MapEntryLock mapLock = new MapEntryLock(this);
            mapLock.setShaddow(shadowMap.get());
            this.mapLocks.set(mapLock);
            tx.addMapEntryLock(mapLock);
        }
    }

    protected boolean isCreateMapCall() {
        return createMapCall;
    }

    protected void setCreateMapCall(boolean createMapCall) {
        this.createMapCall = createMapCall;
    }

    protected String getName(){
        return name;
    }

    //TODO do i need to serialize my inner map also?


    protected void setTimeoutInMilli(int timeoutInMilli) {

        this.timeoutInMilli = timeoutInMilli;
    }
//
//    protected void setMapLocks(MapEntryLock mapLocks) {
//        this.mapLocks.set(mapLocks);
//        mapLocks.setShaddow(map);
//    }

    protected MapEntryLock getMapLocks(){
        return mapLocks.get();
    }


    public int size(){
        if(checkIfGoodTransaction()){
            return map.size();
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
    }

    public boolean isEmpty() {
        if(checkIfGoodTransaction()){
            return  map.isEmpty();
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
        //        if(key.getClass() != keyClass){
//            throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
//        }
//        if(checkIfGoodTransaction() && key.getClass() == keyClass){
//            return map.containsKey(key);
//        }
//        else{
//            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
//        }
    }

    public boolean containsValue(Object value) {
        if(value.getClass() != valueClass){
            throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
        }
        if(checkIfGoodTransaction() && value.getClass() == valueClass){
            return map.containsValue(value);
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
    }

    public V get(Object key){
        logger.info("GET called for K: " + key.toString() );
        if(key.getClass() != keyClass){
            throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
        }
        if(checkIfGoodTransaction() && key.getClass() == keyClass){
            try{
                V getVal = (V)mapLocks.get().Get((K)key);
                return getVal;
            } catch (SystemException e) {
                //TODO probably need to fix this
                throw new ClientTxRolledBackException("There was an issue with serialization");
            }
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
    }

    public V put(K key, V value) {
        logger.info("PUT called for K: " + key.toString() + "  V: " + value.toString());
        if(key.getClass() != keyClass || value.getClass() != valueClass){
            throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
        }
        if(checkIfGoodTransaction()){
            mapLocks.get().put(key, value);
            return shadowMap.get().put(key, value);
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
    }

    public V remove(Object key) {
        logger.info("REMOVE called for K: " + key.toString() );
        if(key.getClass() != keyClass){
            throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
        }
        V rtn = null;
        if(checkIfGoodTransaction() ){
            //TODO what happens here if they remove something in the actual map but i dont have it in the shaddow db?
            //Do i need to first do a get so i can "remove it and send them the value?"


            try {
                //this will return the current value in the real map
                rtn =  (V)mapLocks.get().remove(key);
            } catch (SystemException e) {
                e.printStackTrace();
            }
            if(shadowMap.get().containsKey(key)){
                //if we have the value in the shadow db's map that means that it is more up to date
                //than the one in the real db, according to the client.
                //so we will change the return object to what they currently have in the map
                rtn =  shadowMap.get().remove(key);
            }
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
        return rtn;
    }

    public void putAll(Map m) {
        if(checkIfGoodTransaction()){
            map.putAll(m);
        }
    }

    public void clear() {
        if(checkIfGoodTransaction()){
            map.clear();
        }
    }

    public Set keySet() {
        if(checkIfGoodTransaction()){
            return map.keySet();
        }
        return null;
    }

    public Collection values() {
        if(checkIfGoodTransaction()){
            return map.values();
        }
        return null;
    }

    public Set<Entry<K, V>> entrySet() {
        if(checkIfGoodTransaction()){
            return map.entrySet();
        }
        return null;
    }

    protected boolean checkTypes(Class<K> keyClass, Class<V> valueClass){
        if(this.keyClass == keyClass && this.valueClass == valueClass){
            return true;
        }
        else{
            return false;
        }
    }

    private boolean checkIfGoodTransaction(){
        try {
            switch(TxMgrImpl.Instance.getStatus()){
                case ACTIVE: return true; //mapLocks.validTransaction(txID);
                case UNKNOWN:
                case COMMITTED:
                case ROLLEDBACK:
                case ROLLING_BACK:
                case COMMITTING:
                case NO_TRANSACTION: return false;
            }
        }
        catch (SystemException e){
            return false;
        }
        return  false;
    }

    protected int acquireLock(K key) throws ClientTxRolledBackException{
        logger.info("Acquire lock called for key: " + key.toString());
        try{
            if(locks.containsKey(key)) {
                if(locks.get(key).tryLock(timeoutInMilli, TimeUnit.MILLISECONDS)){
                    logger.info("Lock acquired");
                    int numOfHolds = locks.get(key).getHoldCount();
                    return numOfHolds;
                }
                else {
//                    if(conditionVars.get(key).await(timeoutInMilli, TimeUnit.MILLISECONDS)){
//                        logger.info("Lock acquired");
//                        int numOfHolds = locks.get(key).getHoldCount();
//                        return numOfHolds;
//                    }
                    logger.info("Tried to get lock couldn't");
                    TxMgrImpl.Instance.rollback();
                    throw new ClientTxRolledBackException("Client was timed out");
                }
            }
            else {
                locks.put(key, new ReentrantLock());
                locks.get(key).lock();
                //conditionVars.put(key, locks.get(key).newCondition());
                return 1;
            }
        }
         catch (SystemException e) {
            throw new ClientTxRolledBackException("Client was timed out");
        } catch (InterruptedException e) {
            throw new ClientTxRolledBackException("Client was timed out");
        }
    }

    //TODO maybe i should make the putting into the maps separate from the release of the locks because of rollback
    //Also maybe i should be using condition variables so i can do signal all
    protected void releaseLocks(Set<K> keys) {
       Iterator it =  keys.iterator();
       while(it.hasNext()){
            K toRelease = (K)it.next();
//            Condition con = conditionVars.get(toRelease);
//            con.signal();
            ReentrantLock lock = locks.get(toRelease);
            int numOfHolds = lock.getHoldCount();
            logger.debug("Number of holds on lock: " + numOfHolds);
            for(int i = 0; i < numOfHolds; i++){
                lock.unlock();
                logger.debug("Lock release for Key: " + toRelease.toString());
            }
           logger.info("Lock released for Key: " + toRelease.toString());

       }
       shadowMap.set(null);
       mapLocks.set(null);
    }

    protected void commitRemoves(Set<K> keys){
        Iterator it =  keys.iterator();
        while(it.hasNext()){
            K toRemove = (K)it.next();
            map.remove(toRemove);
        }
    }

    protected void commitPuts(Map<K, V> keysWithValues){
        map.putAll(keysWithValues);
    }

    protected V illegalGet(K key){
        return map.get(key);
    }
}

