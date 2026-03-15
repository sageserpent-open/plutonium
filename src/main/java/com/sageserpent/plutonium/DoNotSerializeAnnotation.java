package com.sageserpent.plutonium;

import com.esotericsoftware.kryo.kryo5.serializers.FieldSerializer;

import java.lang.annotation.Annotation;

// TODO: remove this? I can't find any usage...
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
