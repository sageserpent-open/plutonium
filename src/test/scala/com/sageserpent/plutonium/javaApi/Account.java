package com.sageserpent.plutonium.javaApi;

import com.sageserpent.plutonium.Identified;

public abstract class Account extends Identified {
    public abstract String id();

    public void setCash(double cash){
        this.cash = cash;
    }

    public double getCash(){
        return cash;
    }

    private double cash;
}
