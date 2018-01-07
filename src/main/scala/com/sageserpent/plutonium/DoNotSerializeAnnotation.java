package com.sageserpent.plutonium;

import com.esotericsoftware.kryo.serializers.FieldSerializer;

import java.lang.annotation.Annotation;

public class DoNotSerializeAnnotation implements FieldSerializer.Optional {
    @Override
    public String value() {
        return "doNotSerialize";
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return FieldSerializer.Optional.class;
    }

    public static final DoNotSerializeAnnotation annotation = new DoNotSerializeAnnotation();
}
