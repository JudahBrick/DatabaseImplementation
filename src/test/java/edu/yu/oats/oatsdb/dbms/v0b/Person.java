package edu.yu.oats.oatsdb.dbms.v0b;

import java.io.Serializable;

public class Person implements Serializable {

    private String name;
    private int age;
    private int id;

    Person(String name, int age, int id){
        this.name = name;
        this.age = age;
        this.id = id;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}