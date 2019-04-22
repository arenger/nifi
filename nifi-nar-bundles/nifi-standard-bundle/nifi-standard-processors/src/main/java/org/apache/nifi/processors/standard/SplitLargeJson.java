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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.jsfr.json.JsonPathListener;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.jsfr.json.ParsingContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "split", "jsonpath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("TBD")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All FlowFiles produced from the same parent FlowFile will have the same " +
                        "randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the FlowFiles that were " +
                        "created from a single parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@SeeAlso(SplitJson.class)
public class SplitLargeJson extends AbstractProcessor {

    public static final PropertyDescriptor JSON_PATH_EXPRESSION = new PropertyDescriptor.Builder()
            .name("JsonPath Expression")
            .description("A JsonPath expression that indicates the array element to split into JSON/scalar fragments.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR) // Full validation occurs in #customValidate
            .required(true)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, " +
                    "nothing will be sent to this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid " +
                    "JSON or the specified path does not exist), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(JSON_PATH_EXPRESSION);
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        String value = validationContext.getProperty(JSON_PATH_EXPRESSION).getValue();
        ValidationResult.Builder vrb = new ValidationResult.Builder();
            vrb.valid(true);
        return Collections.singleton(vrb.build());
    }

    private class JsonFragmentWriter implements JsonPathListener {

        private final ProcessSession processSession;
        private final FlowFile original;
        private final Relationship relSplit;
        private int fragmentIndex;
        private ComponentLog logger;
        private Map<String, String> attributes = new HashMap<>();

        public int getCount() {
            return fragmentIndex;
        }

        public JsonFragmentWriter(ProcessSession processSession, FlowFile original, Relationship relSplit,
                                  String groupId, ComponentLog logger){
            this.processSession = processSession;
            this.original = original;
            this.relSplit = relSplit;
            this.logger = logger;
            attributes.put(FRAGMENT_ID.key(), groupId);
            attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), original.getAttribute(CoreAttributes.FILENAME.key()));
        }

        @Override
        public void onValue(Object value, ParsingContext context) {
            FlowFile fragment = processSession.create(original);
            try (OutputStream out = processSession.write(fragment)) {
                out.write(value.toString().getBytes(Charset.defaultCharset()));
            } catch (IOException e) {
                logger.error("Problem writing to flowfile", e);
            }
            attributes.put(FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex++));
            processSession.transfer(processSession.putAllAttributes(fragment, attributes), relSplit);
        }
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) {
        FlowFile original = processSession.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        String groupId = UUID.randomUUID().toString();
        JsonFragmentWriter fragmentWriter =
            new JsonFragmentWriter(processSession, original, REL_SPLIT, groupId, logger);

        try (InputStream is = processSession.read(original)) {
            JsonSurfer surfer = JsonSurferJackson.INSTANCE;
            surfer.configBuilder().bind(
                    processContext.getProperty(JSON_PATH_EXPRESSION).getValue(), fragmentWriter).buildAndSurf(is);
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

        Runtime rt = Runtime.getRuntime();
        rt.gc();
        logger.info(String.format("Memory used: %.3f",
                (double)(rt.totalMemory() - rt.freeMemory())/(1024 * 1024)));

        logger.info("Split {} into {} FlowFile(s)", new Object[]{original, fragmentWriter.getCount()});
        original = copyAttributesToOriginal(processSession, original, groupId, fragmentWriter.getCount());

        processSession.transfer(original, REL_ORIGINAL);
    }
}
