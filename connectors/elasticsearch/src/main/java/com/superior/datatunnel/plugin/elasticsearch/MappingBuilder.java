package com.superior.datatunnel.plugin.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.superior.datatunnel.plugin.elasticsearch.enums.Dynamic;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

import static com.superior.datatunnel.plugin.elasticsearch.MappingParameters.*;

public class MappingBuilder {

    private static final Log LOGGER = LogFactory.getLog(MappingBuilder.class);

    private static final String FIELD_INDEX = "index";

    private static final String FIELD_PROPERTIES = "properties";

    private static final String TYPE_DYNAMIC = "dynamic";

    private static final String TYPE_VALUE_KEYWORD = "keyword";

    private final ObjectMapper objectMapper = new ObjectMapper();

    public MappingBuilder() {
    }

    protected String buildPropertyMapping(EsDataTunnelSinkOption sinkOption) {

        InternalBuilder internalBuilder = new InternalBuilder();
        return internalBuilder.buildPropertyMapping(sinkOption);
    }

    private class InternalBuilder {

        protected String buildPropertyMapping(EsDataTunnelSinkOption sinkOption) {
            try {
                ObjectNode objectNode = objectMapper.createObjectNode();
                mapEntity(objectNode, sinkOption);

                return objectMapper.writer().writeValueAsString(objectNode);
            } catch (IOException e) {
                throw new RuntimeException("could not build mapping", e);
            }
        }

        private void mapEntity(ObjectNode objectNode, EsDataTunnelSinkOption sinkOption) throws IOException {
            if (sinkOption.getDynamic() != Dynamic.INHERIT) {
                objectNode.put(TYPE_DYNAMIC, sinkOption.getDynamic().getMappedName());
            }

            ObjectNode propertiesNode = objectNode.putObject(FIELD_PROPERTIES);
            if (StringUtils.isNotBlank(sinkOption.getIdField())) {
                applyDefaultIdFieldMapping(propertiesNode, sinkOption.getIdField());
            }

            sinkOption.getFields().forEach(field -> {
                addSingleFieldMapping(propertiesNode, field);
            });
        }

        private void applyDefaultIdFieldMapping(ObjectNode propertyNode, String fieldName) {
            propertyNode.set(fieldName, objectMapper.createObjectNode()//
                    .put(FIELD_PARAM_TYPE, TYPE_VALUE_KEYWORD) //
                    .put(FIELD_INDEX, true) //
            );
        }

        /**
         * Add mapping for @Field annotation
         */
        private void addSingleFieldMapping(ObjectNode propertiesNode, FieldMeta field) {
            // build the property json, if empty skip it as this is no valid mapping
            ObjectNode fieldNode = objectMapper.createObjectNode();
            addFieldMappingParameters(fieldNode, field);

            if (fieldNode.isEmpty()) {
                return;
            }

            propertiesNode.set(field.getName(), fieldNode);
        }

        private void addFieldMappingParameters(ObjectNode fieldNode, FieldMeta field) {

            MappingParameters mappingParameters = new MappingParameters(field);

            if (mappingParameters.isStore()) {
                fieldNode.put(FIELD_PARAM_STORE, true);
            }
            mappingParameters.writeTypeAndParametersTo(fieldNode);
        }
    }
}
