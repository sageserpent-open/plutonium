package com.sageserpent.plutonium.javaApi.examples;

import com.sageserpent.plutonium.Identified;

import java.util.Collections;
import java.util.Set;

public class PackageHolder extends Identified {
    private String name;
    private Set<PackageItem> packageItems;

    public PackageHolder(String name){
        this.name = name;
    }

    @Override
    public void checkInvariant(){
        super.checkInvariant();

        for (PackageItem packageItem: packageItems()) {
            final PackageHolder holder = packageItem.holder();
            if (holder != this){
                throw new RuntimeException(holder == null ? "Package item does not know it is being held.": "Package item thinks it is held by something else.");
            }
        }
    }

    @Override
    public String id() {
        return name;
    }

    public Set<PackageItem> packageItems(){
        return Collections.unmodifiableSet(packageItems);
    }

    void hold(PackageItem packageItem){
        packageItems.add(packageItem);
    }

    void release(PackageItem packageItem){
        packageItems.remove(packageItem);
    }
}
