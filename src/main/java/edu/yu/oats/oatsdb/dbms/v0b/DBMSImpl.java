package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public enum DBMSImpl implements ConfigurableDBMS{
    Instance;

    private Map <String, Map> tables;
    private ThreadLocal<Map <String, Map>> tempTables = new ThreadLocal<>();
    //private Map <String, MapProxy> shadowTables;
    //ThreadLocal<Map <String, MapProxy>> shadowTables = new ThreadLocal<>();
    private int txTimeoutInMillis = 0;
    private TxMgr myTxMgr = TxMgrImpl.Instance;
    //private OATSDBType dbType;
    Logger logger = LogManager.getLogger();

    DBMSImpl() {
        tables = new ConcurrentHashMap<>();
        tempTables.set(new ConcurrentHashMap<String, Map>());
    }

    //TODO this is not a good method need to actually check that the types are correct
     /*
     needs a lot of work
      */
    public <K, V> Map<K, V> getMap(String s, Class<K> aClass, Class<V> aClass1){
        TxStatus currentStatus = null;
        MapProxy rtn;
        try{
            currentStatus =  myTxMgr.getStatus();
        }
        catch (SystemException e){
            throw new ClientNotInTxException("There was a system exception, " +
                    "likely because you are not using a transaction manager");
        }
        try {
            String toTestIfEmpty = s;
            toTestIfEmpty.trim();
            if (currentStatus == TxStatus.ACTIVE) {
                if (s == null || s.length() <= 0 || toTestIfEmpty.length() <= 0) {
                    throw new IllegalArgumentException("The name can not be empty");
                }
                rtn = (MapProxy) tables.get(s);
                if (rtn == null) {
                    rtn = (MapProxy) tempTables.get().get(s);
                    if (rtn == null) {
                        throw new NoSuchElementException("There is no map with that name");
                    }
                }
                if (rtn.checkTypes(aClass, aClass1)) {
                    rtn.setUpNewThread((TxImpl) TxMgrImpl.Instance.getTx());
                    return rtn;
//                MapProxy shadow = null;
//                try{
//                    shadow = new MapProxy(aClass, aClass1, s);
//                    MapEntryLock mapLocks = new MapEntryLock(rtn);
//                    shadow.setMapLocks(mapLocks);
//                    shadow.setTimeoutInMilli(txTimeoutInMillis);
//                    ((TxImpl)myTxMgr.getTx()).addMapEntryLock(mapLocks);
//                    shadowTables.get().put(s, shadow);
//                }
//                catch (Exception e){
//                    logger.error("an error was thrown when deserializing");//TODO deal with this
//                }
//                return shadow;
                } else {
                    throw new ClassCastException("Classes used to call the method must be of the same type " +
                            "as the map you are attempting to retrieve");
                }
            }
        }
        catch (SystemException e){
            //please never reach here
        }
        throw new ClientNotInTxException("You must be in a transaction to use this method");

    }

    public <K, V> Map<K, V> createMap(String s, Class<K> aClass, Class<V> aClass1) {
        TxStatus currentStatus = null;
        try{
            currentStatus =  myTxMgr.getStatus();
        }
        catch (SystemException e){
            throw new ClientNotInTxException("There was a system exception");
        }
        String toTestIfEmpty = s;
        toTestIfEmpty.trim();
        if(currentStatus == TxStatus.ACTIVE){
            if(tables.containsKey(s) || tempTables.get().containsKey(s)){// || shadowTables.containsKey(s)){
                throw  new IllegalArgumentException("A map with that name already exists");
            }
            else if(s == null || s.length() <= 0 || toTestIfEmpty.length() <= 0){
                throw  new IllegalArgumentException("The name can not be empty");
            }
            try {
                MapProxy rtn = new MapProxy(aClass, aClass1, s);
                rtn.setTimeoutInMilli(txTimeoutInMillis);
                rtn.setUpNewThread((TxImpl) TxMgrImpl.Instance.getTx());
                tempTables.get().put(s, rtn);
                return rtn;
            }
            catch (Exception e){
                //TODO nothing
                throw new ClientNotInTxException("You must be in a transaction to use this method");
            }
//            MapProxy shadow = null;
//            //tables.put(s, rtn);
//            try{
//                shadow = new MapProxy(aClass, aClass1, s);
//                MapEntryLock mapLocks = new MapEntryLock(rtn);
//                //shadow.setMapLocks(mapLocks);
//                shadow.setUpNewThread((TxImpl) TxMgrImpl.Instance.getTx());
//                //shadow.setTimeoutInMilli(txTimeoutInMillis);
//                //logger.debug("Adding Map lock for map: " + s);
//                //((TxImpl)myTxMgr.getTx()).addMapEntryLock(mapLocks);
//                shadowTables.put(s, shadow);
//            }
//            catch (Exception e){
//                logger.error("an error was thrown when deserializing");//TODO deal with this
//            }
//            return shadow;
        }

        else{
            throw new ClientNotInTxException("You must be in a transaction to use this method");
        }
    }

    protected void addMap(String name, Map map){
        tables.put(name, map);
        tempTables.get().remove(name);
    }

    @Override
    public void setTxTimeoutInMillis(int i) {
        txTimeoutInMillis = i;
    }

    @Override
    public int getTxTimeoutInMillis() {
        return txTimeoutInMillis;
    }

//    protected void setUpShadow(){
//        if(shadowTables == null){
//            shadowTables = new ConcurrentHashMap<String, MapProxy>();
//        }
//    }

    protected void setMapLocksForTransaction(TxImpl tx){
        logger.debug("Adding Map Locks to TX");
        Set<String> keys = tables.keySet();
        Iterator<String> it = keys.iterator();
        while (it.hasNext()){
            logger.debug("Added Map Lock");
            String key = it.next();
            ((MapProxy)tables.get(key)).setUpNewThread(tx);
        }
    }

}

