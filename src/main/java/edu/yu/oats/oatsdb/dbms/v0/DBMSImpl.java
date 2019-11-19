package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public enum DBMSImpl implements DBMS{
    Instance;

    private Map <String, Map> tables;
    //private OATSDBType dbType;
    Logger logger = LogManager.getLogger();

    DBMSImpl() {
        tables = new ConcurrentHashMap<String, Map>();
        //dbType = db;
    }

     //TODO this is not a good method need to actually check that the types are correct
     /*
     needs a lot of work
      */
    public <K, V> Map<K, V> getMap(String s, Class<K> aClass, Class<V> aClass1){

        TxMgr myTxMgr = TxMgrImpl.Instance;
        TxStatus currentStatus = null;
        MapProxy rtn;
        try{
            currentStatus =  myTxMgr.getStatus();
        }
        catch (SystemException e){
            throw new ClientNotInTxException("There was a system exception, " +
                    "likely because you are not using a transaction manager");
        }
        String toTestIfEmpty = s;
        toTestIfEmpty.trim();
        if(currentStatus == TxStatus.ACTIVE){
            if(s == null || s.length() <= 0 || toTestIfEmpty.length() <= 0){
                throw  new IllegalArgumentException("The name can not be empty");
            }
            rtn = (MapProxy) tables.get(s);
            if(rtn == null){
                throw new NoSuchElementException("There is no map with that name");
            }
            if(rtn.checkTypes(aClass, aClass1)){
                return rtn;
            }
            else{
                throw new ClassCastException("Classes used to call the method must be of the same type " +
                        "as the map you are attempting to retrieve");
            }
        }

        throw new ClientNotInTxException("You must be in a transaction to use this method");

    }

    public <K, V> Map<K, V> createMap(String s, Class<K> aClass, Class<V> aClass1) {
        TxMgr myTxMgr = TxMgrImpl.Instance;
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
            if(tables.containsKey(s)){
                throw  new IllegalArgumentException("A map with that name already exists");
            }
            else if(s == null || s.length() <= 0 || toTestIfEmpty.length() <= 0){
                throw  new IllegalArgumentException("The name can not be empty");
            }
            Map rtn = new MapProxy(aClass, aClass1);
            tables.put(s, rtn);
            return rtn;
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction to use this method");
        }
    }
}
