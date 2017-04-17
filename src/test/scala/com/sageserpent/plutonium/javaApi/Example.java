package com.sageserpent.plutonium.javaApi;

import com.sageserpent.plutonium.Identified;

public class Example extends Identified {
    public Example(String id){
        this.id = id;
    }

    public String id() {
        return id;
    }

    public void setAge(int age){
        this.age = age;
    }

    public int getAge(){
        return age;
    }

    private String id;

    private int age;
}
