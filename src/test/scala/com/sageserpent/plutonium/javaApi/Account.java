package com.sageserpent.plutonium.javaApi;

import com.sageserpent.plutonium.Identified;

public class Account extends Identified {
    public Account(String id){
        this.id = id;
    }

    public String id() {
        return id;
    }

    public void setCash(double cash){
        this.cash = cash;
    }

    public double getCash(){
        return cash;
    }

    private String id;

    private double cash;
}
