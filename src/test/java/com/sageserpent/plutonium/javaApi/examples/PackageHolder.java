package com.sageserpent.plutonium.javaApi.examples;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class PackageHolder {
    private Set<PackageItem> packageItems = new HashSet<>();
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

    // TODO: make package-private again once this is supported correctly.
    public void hold(PackageItem packageItem) {
        packageItems.add(packageItem);
    }

    // TODO: make package-private again once this is supported correctly.
    public void release(PackageItem packageItem) {
        packageItems.remove(packageItem);
    }
}
