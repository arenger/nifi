/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.nifi.processors.standard.util.JsonFragmentWriter;
import org.apache.nifi.processors.standard.util.JsonStack;
import org.apache.nifi.processors.standard.util.SimpleJsonPath;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParsingException;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "split", "jsonpath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits a JSON File into multiple, separate FlowFiles for an array element specified by a JsonPath expression. "
        + "Each generated FlowFile is comprised of an element of the specified array and transferred to relationship 'split,' "
        + "with the original file transferred to the 'original' relationship. If the specified JsonPath is not found or "
        + "does not evaluate to an array element, the original file is routed to 'failure' and no files are generated.")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count",
                description = "The number of split FlowFiles generated from the parent FlowFile.  Not available when JSON streaming is enabled."),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "By default, the entire incoming JSON " +
        "document is loaded into memory.  However, if the documents to be processed are large, the JSON content can " +
        "be loaded in a streaming fashion to conserve memory.  The JSON streaming option is faster and uses less " +
        "memory, but only a subset of JSON Path expressions is supported when this option is enabled.  Note, " +
        "however, that a fragment.index attribute is set on every outgoing FlowFile. This, in combination with a " +
        "RouteOnAttribute processor, can be used in place of certain JSON Path constructs. The specified JSON Path " +
        "will be checked and the processor will reflect an invalid state if it is in streaming mode and the " +
        "expression is not supported.")
public class SplitJson extends AbstractJsonPathProcessor {

    public static final PropertyDescriptor ARRAY_JSON_PATH_EXPRESSION = new PropertyDescriptor.Builder()
            .name("JsonPath Expression")
            .description("A JsonPath expression that indicates the array element to split into JSON/scalar fragments.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR) // Full validation/caching occurs in #customValidate
            .required(true)
            .build();

    public static final PropertyDescriptor USE_JSON_STREAMING = new PropertyDescriptor.Builder()
            .name("Use JSON Streaming")
            .description("Conserve memory by processing JSON in a streaming fashion.  The fragment.count attribute " +
                    "will not be added to outgoing FlowFiles when this feature is enabled.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to "
                    + "this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON or the specified "
                    + "path does not exist), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private final AtomicReference<JsonPath> JSON_PATH_REF = new AtomicReference<>();
    private volatile String nullDefaultValue;
    private volatile boolean useJsonStreaming;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ARRAY_JSON_PATH_EXPRESSION);
        properties.add(NULL_VALUE_DEFAULT_REPRESENTATION);
        properties.add(USE_JSON_STREAMING);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SPLIT);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.equals(ARRAY_JSON_PATH_EXPRESSION)) {
            if (!StringUtils.equals(oldValue, newValue)) {
                if (oldValue != null) {
                    // clear the cached item
                    JSON_PATH_REF.set(null);
                }
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        JsonPathValidator validator = new JsonPathValidator() {
            @Override
            public void cacheComputedValue(String subject, String input, JsonPath computedJson) {
                JSON_PATH_REF.set(computedJson);
            }

            @Override
            public boolean isStale(String subject, String input) {
                return JSON_PATH_REF.get() == null;
            }
        };

        String value = validationContext.getProperty(ARRAY_JSON_PATH_EXPRESSION).getValue();
        ValidationResult result = validator.validate(ARRAY_JSON_PATH_EXPRESSION.getName(), value, validationContext);
        if (result.isValid() && Boolean.parseBoolean(validationContext.getProperty(USE_JSON_STREAMING).getValue())) {
            try {
                SimpleJsonPath.of(value);
            } catch (Exception e) {
                ValidationResult.Builder vrb = new ValidationResult.Builder();
                vrb.subject(value);
                vrb.input(getClass().getName());
                vrb.explanation("the specified JSON path is not supported in streaming mode.");
                result = vrb.build();
            }
        }
        return Collections.singleton(result);
    }

    @OnScheduled
    public void onScheduled(ProcessContext processContext) {
        nullDefaultValue = NULL_REPRESENTATION_MAP.get(processContext.getProperty(NULL_VALUE_DEFAULT_REPRESENTATION).getValue());
        useJsonStreaming = Boolean.parseBoolean(processContext.getProperty(USE_JSON_STREAMING).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) {
        final JsonPath jsonPath = JSON_PATH_REF.get();

        if (useJsonStreaming) {
            processSimplePath(processSession, jsonPath.getPath());
        } else {
            processComplexPath(processSession, jsonPath);
        }
    }

    private void processComplexPath(final ProcessSession processSession, final JsonPath jsonPath) {
        FlowFile original = processSession.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        DocumentContext documentContext;
        try {
            documentContext = validateAndEstablishJsonContext(processSession, original);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{original});
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        Object jsonPathResult;
        try {
            jsonPathResult = documentContext.read(jsonPath);
        } catch (PathNotFoundException e) {
            logger.warn("JsonPath {} could not be found for FlowFile {}", new Object[]{jsonPath.getPath(), original});
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        if (!(jsonPathResult instanceof List)) {
            logger.error("The evaluated value {} of {} was not a JSON Array compatible type and cannot be split.", new Object[]{jsonPathResult, jsonPath.getPath()});
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        List resultList = (List) jsonPathResult;

        Map<String, String> attributes = new HashMap<>();
        final String fragmentId = UUID.randomUUID().toString();
        attributes.put(FRAGMENT_ID.key(), fragmentId);
        attributes.put(FRAGMENT_COUNT.key(), Integer.toString(resultList.size()));

        for (int i = 0; i < resultList.size(); i++) {
            Object resultSegment = resultList.get(i);
            FlowFile split = processSession.create(original);
            split = processSession.write(split, (out) -> {
                        String resultSegmentContent = getResultRepresentation(resultSegment, nullDefaultValue);
                        out.write(resultSegmentContent.getBytes(StandardCharsets.UTF_8));
                    }
            );
            attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), split.getAttribute(CoreAttributes.FILENAME.key()));
            attributes.put(FRAGMENT_INDEX.key(), Integer.toString(i));
            processSession.transfer(processSession.putAllAttributes(split, attributes), REL_SPLIT);
        }

        original = copyAttributesToOriginal(processSession, original, fragmentId, resultList.size());
        processSession.transfer(original, REL_ORIGINAL);
        logger.info("Split {} into {} FlowFiles", new Object[]{original, resultList.size()});
    }

    private void processSimplePath(final ProcessSession processSession, final String jsonPath) {
        FlowFile original = processSession.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        String groupId = UUID.randomUUID().toString();
        SimpleJsonPath simplePath;
        try {
            simplePath = SimpleJsonPath.of(jsonPath) ;
        } catch (Exception e) {
            logger.error("Json path incompatible with streaming mode: {}", new Object[]{jsonPath});
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        JsonFragmentWriter fragmentWriter = new JsonFragmentWriter(processSession, original, REL_SPLIT, groupId,
                simplePath.hasWildcard() ? JsonFragmentWriter.Mode.DISJOINT : JsonFragmentWriter.Mode.CONTIGUOUS);
        JsonStack stack = new JsonStack();
        JsonFragmentWriter.BoolPair section = new JsonFragmentWriter.BoolPair();
        if (simplePath.isEmpty()) {
            section.push(true);
        }

        try (InputStream is = processSession.read(original); JsonParser parser = Json.createParser(is)) {
            JsonParserView view = new JsonParserView(parser);
            while (parser.hasNext()) {
                JsonParser.Event e = parser.next();
                stack.receive(e, view);
                section.push(stack.startsWith(simplePath));
                fragmentWriter.filterEvent(section, e, view, stack.size());
            }
        } catch (JsonParsingException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{original});
            processSession.transfer(original, REL_FAILURE);
            return;
        } catch (Exception e) {
            logger.error("Problems with FlowFile {}: {}", new Object[]{original, e.getMessage()});
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        if (fragmentWriter.getCount() == 0) {
            logger.error("No object or array was found at the specified json path");
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        logger.info("Split {} into {} FlowFile(s)", new Object[]{original, fragmentWriter.getCount()});
        original = copyAttributesToOriginal(processSession, original, groupId, fragmentWriter.getCount());
        processSession.transfer(original, REL_ORIGINAL);
    }


    public class JsonParserView {
        private final JsonParser parser;

        JsonParserView(JsonParser parser) {
            this.parser = parser;
        }

        public String getString() {
            return parser.getString();
        }

        public BigDecimal getBigDecimal() {
            return parser.getBigDecimal();
        }

        public int getInt() {
            return parser.getInt();
        }

        public boolean isIntegralNumber() {
            return parser.isIntegralNumber();
        }
    }
}