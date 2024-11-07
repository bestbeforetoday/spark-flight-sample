/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class OptionsBuilderTest {
    private static final String API_HOST = "api.host.name";
    private static final Project PROJECT = new Project("project_id");
    private static final DataAsset ASSET = new DataAsset("asset_id", PROJECT);
    private static final Random RANDOM = new Random();

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void flight_command_includes_discovered_fields() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();
        var fields = mapper.readValue(
                "[ {"
                        + "  \"name\": \"ID\","
                        + "  \"type\": {"
                        + "    \"type\": \"bigint\","
                        + "    \"length\": -1,"
                        + "    \"scale\": 0,"
                        + "    \"nullable\": true,"
                        + "    \"signed\": true"
                        + "} } ]",
                ArrayNode.class);
        discovery.set("fields", fields);

        var builder = OptionsBuilder.forAsset(API_HOST, discovery, ASSET);
        var options = builder.build();

        var actual = getFlightCommand(options).get("fields");
        assertThat(actual).isEqualTo(discovery.get("fields"));
    }

    @Test
    void flight_command_includes_discovered_datasource_name() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();
        var datasourceType = mapper.readValue("{ \"entity\": { \"name\": \"datasource_name\" } }", ObjectNode.class);
        discovery.set("datasource_type", datasourceType);

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.build();

        var actual = getFlightCommand(options).get("datasource_type").asText();
        assertThat(actual).isEqualTo("datasource_name");
    }

    @Test
    void flight_command_includes_discovered_connection_properties() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();
        var connectionProps = mapper.readValue(
                "{ \"bucket\": \"bucket_name\", \"url\": \"https://s3.domain.name\" }", ObjectNode.class);
        discovery.set("connection_properties", connectionProps);

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.build();

        var actual = getFlightCommand(options).get("connection_properties");
        assertThat(actual).isEqualTo(connectionProps);
    }

    @Test
    void flight_command_includes_discovered_interaction_properties() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();
        var interactionProps =
                mapper.readValue("{ \"bucket\": \"bucket_name\", \"file_name\" : \"file/name\" }", ObjectNode.class);
        discovery.set("interaction_properties", interactionProps);

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.build();

        var actual = getFlightCommand(options).get("interaction_properties");
        assertThat(actual).isEqualTo(interactionProps);
    }

    @Test
    void flight_command_includes_asset() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();

        var builder = OptionsBuilder.forAsset(API_HOST, discovery, ASSET);
        var options = builder.build();

        var actual = getFlightCommand(options);
        assertThat(actual.get(ASSET.key()).asText()).isEqualTo(ASSET.id());
        assertThat(actual.get(ASSET.container().key()).asText())
                .isEqualTo(ASSET.container().id());
    }

    @Test
    void flight_location_URL_uses_API_host() {
        var discovery = mapper.createObjectNode();

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.build();

        var actual = URI.create(options.get("flight.location")).getHost();
        assertThat(actual).isEqualTo(API_HOST);
    }

    @Test
    void forConnection_has_target_context() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.build();

        var actual = getFlightCommand(options).get("context").asText();
        assertThat(actual).isEqualTo("target");
    }

    @Test
    void forAsset_has_source_or_default_context() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();

        var builder = OptionsBuilder.forAsset(API_HOST, discovery, ASSET);
        var options = builder.build();

        var actual = getFlightCommand(options).get("context").asText();
        assertThat(actual).satisfiesAnyOf(value -> assertThat(value).isEqualTo("source"), value -> assertThat(value)
                .isNull());
    }

    @Test
    void numPartitions() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();
        var expected = nextPositiveInt();

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.numPartitions(expected).build();

        var actual = getFlightCommand(options).get("num_partitions").asInt();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void batchSize() throws JsonProcessingException {
        var discovery = mapper.createObjectNode();
        var expected = nextPositiveInt();

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.batchSize(expected).build();

        var actual = getFlightCommand(options).get("batch_size").asInt();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void timeout() {
        var discovery = mapper.createObjectNode();
        var expected = nextPositiveInt() + "s";

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.timeout(expected).build();

        var actual = options.get("flight.timeout.default");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void accessToken() {
        var discovery = mapper.createObjectNode();
        var expected = "ACCESS_TOKEN";

        var builder = OptionsBuilder.forConnection(API_HOST, discovery);
        var options = builder.accessToken(expected).build();

        var actual = options.get("flight.authToken");
        assertThat(actual).isEqualTo(expected);
    }

    private ObjectNode getFlightCommand(final Map<String, String> options) throws JsonProcessingException {
        var command = options.get("flight.command");
        assertThat(command).withFailMessage("flight.command is null").isNotNull();

        return mapper.readValue(command, ObjectNode.class);
    }

    private int nextPositiveInt() {
        return Math.abs(RANDOM.nextInt());
    }
}
