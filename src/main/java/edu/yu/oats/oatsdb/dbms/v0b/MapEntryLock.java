package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.SystemException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
//extends Serializable
class MapEntryLock<K, V> {

    private MapProxy<K, V > theRealMap;
    private Map<K, V> shaddow = new HashMap<>();
    private Set<K> removes;
    private Set<K> removedFromRemoves;
    private Set<K> keysWithLock;
    Logger logger  = LogManager.getLogger();

    protected MapEntryLock(MapProxy map){
        theRealMap = map;
        removes = new HashSet<>();
        removedFromRemoves = new HashSet<>();
        keysWithLock = new HashSet<>();
    }

    protected V Get(K key) throws SystemException {
        int numOfHolds = theRealMap.acquireLock(key);
        keysWithLock.add(key);
        if(shaddow.containsKey(key)){
            return shaddow.get(key);
        }
        else if(removes.contains(key) || !shaddow.containsKey(key) && numOfHolds > 1  && removedFromRemoves.contains(key)){
            return null;
        }
        V val = deepCopy(theRealMap.illegalGet(key));
        if(val != null){
            shaddow.put(key, val);
        }
        return val;
    }

    //TODO maybe we should return the value of putting here or maybe just use aquire lock and get rid of the value paramater
    protected void put(K key, V value){
        theRealMap.acquireLock(key);
        keysWithLock.add(key);
        //if we had previously had a remove for this key we need to get rid of that.
        // alternatively on commit we can do all the removes and then all the puts, maybe?
        //TODO (read comment above)
        if(removes.contains(key)){
            removes.remove(key);
            removedFromRemoves.add(key);
        }
    }

    protected V remove(K key) throws SystemException{
        theRealMap.acquireLock(key);
        keysWithLock.add(key);
        if(theRealMap.containsKey(key)){
            removes.add(key);
            return deepCopy(theRealMap.illegalGet(key));
        }
        return null;
    }

    public void setShaddow(Map<K, V> shaddow) {
        this.shaddow = shaddow;
    }

    private void releaseTheKraken(){
       //TODO should only need to give the set of keys i hold the lock on
        theRealMap.releaseLocks(keysWithLock);
        shaddow.clear();
        removes.clear();
        keysWithLock.clear();
    }

    protected void commit() throws SystemException {
        /*
           maybe we should do it in this order:
           do all the removes
           do all the puts
           release all the locks
         */
        logger.debug("        COMMIT IN MAP LOCK   "  + Thread.currentThread().getName());
        if(mapIsSerializable()){
            theRealMap.commitRemoves(removes);
            theRealMap.commitPuts(shaddow);

            if(theRealMap.isCreateMapCall()){
                DBMSImpl.Instance.addMap(theRealMap.getName(), theRealMap);
            }
            theRealMap.setCreateMapCall(false);
            //should be last thing called
            releaseTheKraken();
        }

    }

    protected void rollback(){
        logger.debug("        ROLLBACK IN MAP LOCK   "  + Thread.currentThread().getName());
        //should be last thing called
        releaseTheKraken();
    }

//    protected boolean validTransaction(UUID id){
//        return id == TxMgrImpl.Instance.getTxId();
//    }

    private boolean mapIsSerializable() throws SystemException {
        try{
            Set<K> keys = shaddow.keySet();
            Iterator it = keys.iterator();
            while (it.hasNext()){
                K currentKey = (K)it.next();
                V val = shaddow.get(currentKey);
                shaddow.put(currentKey, deepCopy(val));
            }
        }
        catch (Exception e){
            TxMgrImpl.Instance.rollback();
            throw new SystemException("Serialization issue with this map");
        }
        return true;
    }

    private V deepCopy(V val) throws  SystemException {
        //Code taken from  https://howtodoinjava.com/java/serialization/how-to-do-deep-cloning-using-in-memory-serialization-in-java/
        //Serialization of object
        V copied = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(val);

            //De-serialization of object
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream in = new ObjectInputStream(bis);
            copied = (V) in.readObject();
        }
        catch (Exception e){
            //TODO this
            throw new SystemException("Serialization issue");
        }
        return copied;
    }
}
