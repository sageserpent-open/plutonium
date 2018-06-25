package com.sageserpent.plutonium.javaApi.examples;

import scala.collection.mutable.HashSet;

import java.util.Collections;
import scala.collection.JavaConverters$;

import java.util.Set;

public abstract class PackageHolder {
    private Set<PackageItem> packageItems = JavaConverters$.MODULE$.mutableSetAsJavaSet(new HashSet());
    private String location;

    public abstract String id();

    public void checkInvariant() {
        for (PackageItem packageItem : packageItems()) {
            final PackageHolder holder = packageItem.holder();
            if (holder != this) {
                throw new RuntimeException(holder ==
                                                   null ? "Package item " +
                        "does not know it is being held." : "Package item " +
                        "thinks it is held by something else.");
            }
        }
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Set<PackageItem> packageItems() {
        return Collections.unmodifiableSet(packageItems);
    }

    void hold(PackageItem packageItem) {
        packageItems.add(packageItem);
    }

    void release(PackageItem packageItem) {
        packageItems.remove(packageItem);
    }
}
