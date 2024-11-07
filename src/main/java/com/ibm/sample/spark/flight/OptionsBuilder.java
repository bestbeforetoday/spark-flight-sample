/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Builder for Spark Flight options, used when reading or writing data with Spark. The options are generated using
 * information discovered from the Flight service.
 *
 * <p>Instances of this class are obtained from {@link Discovery}.
 */
public final class OptionsBuilder {
    private final String apiHost;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectNode command = mapper.createObjectNode();
    private String timeout;
    private String accessToken;

    static OptionsBuilder forConnection(final String apiHost, final ObjectNode discovery) {
        return new OptionsBuilder(apiHost, discovery).context(Context.TARGET);
    }

    static OptionsBuilder forAsset(final String apiHost, final ObjectNode discovery, final DataAsset asset) {
        return new OptionsBuilder(apiHost, discovery).asset(asset).context(Context.SOURCE);
    }

    private OptionsBuilder(final String apiHost, final ObjectNode discovery) {
        this.apiHost = apiHost;

        // Properties from Connection / path discovery
        Optional.ofNullable(discovery.get("datasource_type"))
                .map(type -> type.get("entity"))
                .map(entity -> entity.get("name"))
                .ifPresent(name -> command.set("datasource_type", name));
        Optional.ofNullable(discovery.get("connection_properties"))
                .ifPresent(props -> command.set("connection_properties", props));
        Optional.ofNullable(discovery.get("interaction_properties"))
                .ifPresent(props -> command.set("interaction_properties", props));

        // fields are present in Asset discovery
        command.set("fields", discovery.get("fields"));
    }

    private OptionsBuilder asset(final DataAsset asset) {
        command.put(asset.key(), asset.id());
        command.put(asset.container().key(), asset.container().id());
        return this;
    }

    /**
     * The maximum number of partitions that can be used for parallelism in table reading and writing.
     *
     * @param value The number of partitions.
     * @return This builder instance.
     */
    public OptionsBuilder numPartitions(final int value) {
        command.put("num_partitions", value);
        return this;
    }

    /**
     * The batch size, which determines how many rows to transfer per round trip.
     *
     * @param value The batch size.
     * @return This builder instance.
     */
    public OptionsBuilder batchSize(final int value) {
        command.put("batch_size", value);
        return this;
    }

    private OptionsBuilder context(final Context value) {
        command.put("context", value.toString());
        return this;
    }

    /**
     * Timeout of the form {@code "60s"}.
     *
     * @param value timeout period.
     * @return this.
     */
    public OptionsBuilder timeout(final String value) {
        timeout = value;
        return this;
    }

    /**
     * Access token used to interact with the Flight service.
     *
     * @param value An access token.
     * @return This builder instance.
     */
    public OptionsBuilder accessToken(final String value) {
        accessToken = value;
        return this;
    }

    /**
     * Build Spark options from the configured builder state.
     *
     * @return Spark options.
     */
    public Map<String, String> build() {
        var result = new HashMap<String, String>();
        result.put("flight.location", "grpc+tls://" + apiHost + ":443");
        result.put("flight.command", command.toString());
        if (timeout != null) {
            result.put("flight.timeout.default", timeout);
        }
        result.put("flight.useTls", "true");
        if (accessToken != null) {
            result.put("flight.authToken", accessToken);
        }

        return result;
    }
}
