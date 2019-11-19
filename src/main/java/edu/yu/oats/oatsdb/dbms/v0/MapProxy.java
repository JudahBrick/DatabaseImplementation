package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.ClientNotInTxException;
import edu.yu.oats.oatsdb.dbms.SystemException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MapProxy <K, V> implements Map<K, V> {

   private Map<K, V> map;
   private Class<K> keyClass;
   private Class<V> valueClass;


   protected MapProxy(Class<K> class1, Class<V> class2){
       keyClass = class1;
       valueClass = class2;
       map =  new ConcurrentHashMap<K, V>();

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
       if(key.getClass() != keyClass){
           throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
       }
        if(checkIfGoodTransaction() && key.getClass() == keyClass){
            return map.containsKey(key);
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
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

    public V get(Object key) {
       if(key.getClass() != keyClass){
           throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
       }
        if(checkIfGoodTransaction() && key.getClass() == keyClass){
            return map.get(key);
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
    }

    public V put(K key, V value) {
        if(key.getClass() != keyClass || value.getClass() != valueClass){
            throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
        }
        if(checkIfGoodTransaction()){
            return map.put(key, value);
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
    }


    public V remove(Object key) {
        if(key.getClass() != keyClass){
            throw new IllegalArgumentException("Must be consistent in Type with Keys and Values");
        }
        if(checkIfGoodTransaction() ){
            return map.remove(key);
        }
        else{
            throw new ClientNotInTxException("You must be in a transaction in order to access Map functions");
        }
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
               case ACTIVE: return true;
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
}
