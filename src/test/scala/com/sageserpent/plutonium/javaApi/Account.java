package com.sageserpent.plutonium.javaApi;

public abstract class Account {
    public abstract String id();

    public void setCash(double cash){
        this.cash = cash;
    }

    public double getCash(){
        return cash;
    }

    private double cash;
}
