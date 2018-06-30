package com.sageserpent.plutonium.javaApi.examples;

import java.util.*;

public abstract class PackageHolder {
    private Collection<PackageItem> packageItems = new LinkedList<>();
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

    public Collection<PackageItem> packageItems() {
        return Collections.unmodifiableCollection(packageItems);
    }

    void hold(PackageItem packageItem) {
        packageItems.add(packageItem);
    }

    void release(PackageItem packageItem) {
        packageItems.remove(packageItem);
    }
}
