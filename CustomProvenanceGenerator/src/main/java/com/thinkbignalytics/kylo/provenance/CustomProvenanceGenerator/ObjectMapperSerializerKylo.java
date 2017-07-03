package com.thinkbignalytics.kylo.provenance.CustomProvenanceGenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 */
public class ObjectMapperSerializerKylo {

    private static final Logger log = LoggerFactory.getLogger(ObjectMapperSerializerKylo.class);


    private ObjectMapper mapper;

    public ObjectMapperSerializerKylo() {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new JodaModule());
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public String serialize(Object obj) {
        String json = null;
        try {
            json = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing object", e);
        }
        return json;
    }

    public <T> T deserialize(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Error de-serializing object", e);
        }
    }
}
