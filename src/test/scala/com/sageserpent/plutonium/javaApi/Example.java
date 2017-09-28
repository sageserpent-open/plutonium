package com.sageserpent.plutonium.javaApi;

import com.sageserpent.plutonium.Identified;

public abstract class Example extends Identified {
    public abstract String id();

    public void setAge(int age){
        this.age = age;
    }

    public int getAge(){
        return age;
    }

    private int age;
}
