/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.cloud;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

/** Helper class for authentication with IBM Cloud, allowing access tokens to be obtained. */
public final class Authentication {
    private final HttpClient client;
    private final URI authEndpoint;

    /**
     * Constructor.
     *
     * @param client An HTTP client used to access the Flight service.
     * @param authEndpoint An endpoint address used for authentication.
     */
    public Authentication(final HttpClient client, final URI authEndpoint) {
        this.client = client;
        this.authEndpoint = authEndpoint;
    }

    /**
     * Request a new access token using a specified API key.
     *
     * @param apiKey An API key.
     * @return A new access token.
     * @throws IOException if an error occurs invoking the authorization service.
     * @throws InterruptedException if the operation was terminated unexpectedly.
     */
    public String accessToken(final String apiKey) throws IOException, InterruptedException {
        var request = newAccessTokenRequest(apiKey);
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Responses.assertSuccess(response);
        return readAccessToken(response);
    }

    private HttpRequest newAccessTokenRequest(final String apiKey) {
        // curl --request POST \
        // --url https://iam.cloud.ibm.com/identity/token \
        // --header 'Accept: application/json' \
        // --header 'Content-Type: application/x-www-form-urlencoded' \
        // --data apikey=<<<API KEY>>>> \
        // --data grant_type=urn:ibm:params:oauth:grant-type:apikey
        var body = formDataAsString(Map.ofEntries(
                Map.entry("apikey", apiKey), Map.entry("grant_type", "urn:ibm:params:oauth:grant-type:apikey")));
        return HttpRequest.newBuilder()
                .uri(authEndpoint)
                .header("Accept", "application/json")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private String formDataAsString(final Map<String, String> formData) {
        return formData.entrySet().stream().map(this::formEntryAsString).collect(Collectors.joining("&"));
    }

    private String formEntryAsString(final Map.Entry<String, String> entry) {
        return URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                + '='
                + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8);
    }

    private String readAccessToken(final HttpResponse<String> responseJson) throws IOException {
        return Responses.parseJsonObject(responseJson).get("access_token").asText();
    }
}
