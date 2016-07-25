package com.sageserpent.plutonium.javaApi.examples;

import com.sageserpent.plutonium.Identified;


public class PackageItem extends Identified{
    private final long orderNumber;
    private PackageHolder holder;
    private String intendedDestination;
    private String actualDestination;

    private double valuePaid = 0.0;
    private boolean isWrongItem = false;

    public PackageItem(long orderNumber){
        this.orderNumber = orderNumber;
    }

    @Override
    public Long id() {
        return orderNumber;
    }

    @Override
    public void checkInvariant(){
        super.checkInvariant();

        if (isHeld() && !holder.packageItems().contains(this)){
            throw new RuntimeException("Holder does not know it is holding this package item.");
        }

        // NOTE: it *is* possible for an item to be neither held nor delivered,
        // this is the initial state post-construction.
        if (hasBeenDelivered() && isHeld()){
            throw new RuntimeException("A delivered item should not be considered as being held.");
        }
    }

    public boolean hasBeenDelivered(){
        return null != actualDestination;
    }

    public boolean isHeld() {
        return null != holder;
    }

    public boolean isWrongItem(){
        return isWrongItem;
    }

    public boolean hasBeenDeliveredToTheWrongDestination(){
        return hasBeenDelivered() && getIntendedDestination() != actualDestination();
    }

    public void recordDelivery(){
        if (hasBeenDelivered()){
            throw new RuntimeException("Precondition violated: cannot record delivery of an item that has already been delivered.");
        }

        if (null == intendedDestination){
            throw new RuntimeException("Must have an intended destination for it to have been delivered to.");
        }

        heldBy(null);

        actualDestination = intendedDestination;
    }

    public void recordThatPackageWasWrongItem(){
        isWrongItem = true;
    }

    public void recordDeliveryWasToWrongDestination(String actualDestination) {
        if (!hasBeenDelivered()){
            throw new RuntimeException("Precondition violated: cannot record delivery to wrong destination unless item was actually delivered.");
        }

        if (actualDestination == intendedDestination){
            throw new RuntimeException("If the actual destination is the intended one, then it can't be wrongly delivered.");
        }

        this.actualDestination = actualDestination;
    }

    public void setIntendedDestination(String intendedDestination) {
        this.intendedDestination = intendedDestination;
    }

    public String getIntendedDestination(){
        return intendedDestination;
    }

    public String actualDestination() {
        return actualDestination;
    }

    public PackageHolder holder(){
        return holder;
    }

    public void heldBy(PackageHolder holder){
        if (holder != this.holder) {
            PackageHolder previousHolder = this.holder;

            if (null != holder) {
                holder.hold(this);
                actualDestination = null;
            }

            if (null != previousHolder){
                previousHolder.release(this);
            }

            this.holder = holder;
        }
    }


    public double getValuePaid() {
        return valuePaid;
    }

    public void setValuePaid(double valuePaid) {
        this.valuePaid = valuePaid;
    }
}