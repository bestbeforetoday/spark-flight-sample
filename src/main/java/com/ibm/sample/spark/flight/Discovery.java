/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.sample.cloud.Responses;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Used to discover asset information from the Flight service. */
public final class Discovery {
    private final HttpClient client;
    private final String apiHost;
    private final URI apiRoot;
    private String accessToken;

    /** Results of a discovery invocation. */
    public interface Result {
        /**
         * Get the raw data returned from the Flight discovery invocation.
         *
         * @return Flight response data.
         */
        ObjectNode getFlightData();

        /**
         * Build Spark options from the discovered Flight data.
         *
         * @return A Spark options builder.
         */
        OptionsBuilder options();
    }

    /**
     * Constructor.
     *
     * @param client An HTTP client used to access the Flight service.
     * @param apiHost The host name providing the Flight service.
     * @throws NullPointerException if the HTTP client is null.
     */
    public Discovery(final HttpClient client, final String apiHost) {
        Objects.requireNonNull(client);

        this.client = client;
        this.apiHost = apiHost;
        this.apiRoot = URI.create("https://" + apiHost + "/v2/");
    }

    /**
     * Set the access token used to interact with the Flight service.
     *
     * @param value An access token.
     */
    public void setAccessToken(final String value) {
        accessToken = value;
    }

    /**
     * Discover information required to access a specific data asset.
     *
     * @param asset A data asset reference.
     * @return Discovered information about the asset.
     * @throws IOException If an error occurs invoking the Flight service.
     * @throws InterruptedException If the operation is interrupted unexpectedly.
     */
    public Result discover(final DataAsset asset) throws IOException, InterruptedException {
        var request = URI.create("connections/assets/" + encode(asset.id()) + "?"
                + asset.container().key() + "="
                + encode(asset.container().id())
                + "&fetch=metadata"
                + "&context=" + Context.SOURCE);
        var result = send(request);

        return new Result() {
            @Override
            public ObjectNode getFlightData() {
                return result;
            }

            @Override
            public OptionsBuilder options() {
                return OptionsBuilder.forAsset(apiHost, result, asset);
            }
        };
    }

    /**
     * Discover information required to access data located at a given path using a specific connection.
     *
     * @param connection A connection reference.
     * @param path Location of the asset.
     * @return Discovered information about the asset.
     * @throws IOException If an error occurs invoking the Flight service.
     * @throws InterruptedException If the operation is interrupted unexpectedly.
     */
    public Result discover(final Connection connection, final String path) throws IOException, InterruptedException {
        var request = URI.create("connections/" + encode(connection.id()) + "/assets?"
                + connection.container().key() + "="
                + encode(connection.container().id())
                + "&path=" + encode(path)
                + "&fetch=datasource_type,connection,interaction"
                + "&context=" + Context.TARGET);
        var result = send(request);

        return new Result() {
            @Override
            public ObjectNode getFlightData() {
                return result;
            }

            @Override
            public OptionsBuilder options() {
                return OptionsBuilder.forConnection(apiHost, result);
            }
        };
    }

    private ObjectNode send(final URI path) throws IOException, InterruptedException {
        var uri = apiRoot.resolve(path);
        var request = newRequest(uri);
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Responses.assertSuccess(response);
        return Responses.parseJsonObject(response);
    }

    private HttpRequest newRequest(final URI uri) {
        var builder = HttpRequest.newBuilder().uri(uri).GET();

        if (accessToken != null) {
            var authHeader = "Bearer " + accessToken;
            builder.header("Authorization", authHeader);
        }

        return builder.build();
    }

    private String encode(final String urlFragment) {
        return URLEncoder.encode(urlFragment, StandardCharsets.UTF_8);
    }
}
