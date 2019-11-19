package edu.yu.oats.oatsdb.dbms.v0b;

import java.io.Serializable;

public class BankAccount implements Serializable {
    String name;
    int amount;

    BankAccount(String name){
        this.name = name;
        amount = 0;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addMoney(int n){
        amount += n;
    }

    public void withdrawMoney(int n){
        amount -= n;
    }

    public int getAmount(){
        return amount;
    }
}
