/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DiscoveryTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Project project = new Project("project_id");
    private final DataAsset dataAsset = new DataAsset("asset_id", project);
    private final Connection connection = new Connection("connection_id", project);

    @Test
    void constructor_throws_NullPointerException_if_HTTP_client_is_null() {
        assertThatThrownBy(() -> newDiscovery(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void discover_asset_returns_response() throws IOException, InterruptedException {
        var expected = expectedResponse();
        var mockClient = newMockClient(200, expected);
        var discovery = newDiscovery(mockClient);

        var actual = discovery.discover(dataAsset).getFlightData();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void discover_asset_throws_on_error_HTTP_status() throws IOException, InterruptedException {
        var mockClient = newMockClient(400, expectedResponse());
        var discovery = newDiscovery(mockClient);

        assertThatThrownBy(() -> discovery.discover(dataAsset)).isInstanceOf(IOException.class);
    }

    @Test
    void asset_ID_is_URL_encoded() throws IOException, InterruptedException {
        var mockClient = newMockClient(200, expectedResponse());
        var discovery = newDiscovery(mockClient);

        var asset = new DataAsset("a b", project);
        discovery.discover(asset);

        var actual = captureRequest(mockClient).uri().getPath();
        assertThat(actual).endsWith("/connections/assets/a+b");
    }

    @Test
    void asset_project_ID_is_URL_encoded() throws IOException, InterruptedException {
        var mockClient = newMockClient(200, expectedResponse());
        var discovery = newDiscovery(mockClient);

        var project = new Project("asset id");
        var asset = new DataAsset("asset_id", project);
        discovery.discover(asset);

        var actual = captureRequest(mockClient).uri().getQuery();
        assertThat(actual).contains(project.key() + "=asset+id");
    }

    @Test
    void discover_connection_path_returns_response() throws IOException, InterruptedException {
        var expected = expectedResponse();
        var mockClient = newMockClient(200, expected);
        var discovery = newDiscovery(mockClient);

        var actual = discovery.discover(connection, "path").getFlightData();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void discover_connection_path_throws_on_error_HTTP_status() throws IOException, InterruptedException {
        var mockClient = newMockClient(400, expectedResponse());
        var discovery = newDiscovery(mockClient);

        assertThatThrownBy(() -> discovery.discover(connection, "path")).isInstanceOf(IOException.class);
    }

    @Test
    void connection_ID_is_URL_encoded() throws IOException, InterruptedException {
        var mockClient = newMockClient(200, expectedResponse());
        var discovery = newDiscovery(mockClient);

        var connection = new Connection("connection id", project);
        discovery.discover(connection, "path");

        var actual = captureRequest(mockClient).uri().getPath();
        assertThat(actual).endsWith("/connections/connection+id/assets");
    }

    @Test
    void connection_project_ID_is_URL_encoded() throws IOException, InterruptedException {
        var mockClient = newMockClient(200, expectedResponse());
        var discovery = newDiscovery(mockClient);

        var project = new Project("project id");
        var connection = new Connection("connection_id", project);
        discovery.discover(connection, "path");

        var actual = captureRequest(mockClient).uri().getQuery();
        assertThat(actual).contains(project.key() + "=project+id");
    }

    @Test
    void path_is_URL_encoded() throws IOException, InterruptedException {
        var mockClient = newMockClient(200, expectedResponse());
        var discovery = newDiscovery(mockClient);

        discovery.discover(connection, "asset path");

        var actual = captureRequest(mockClient).uri().getQuery();
        assertThat(actual).contains("path=asset+path");
    }

    @Test
    void set_access_token() throws IOException, InterruptedException {
        var expected = "MY_ACCESS_TOKEN";
        var mockClient = newMockClient(200, expectedResponse());
        var discovery = newDiscovery(mockClient);

        discovery.setAccessToken(expected);
        discovery.discover(dataAsset);

        var actual = captureRequest(mockClient).headers().firstValue("Authorization");
        assertThat(actual).hasValue("Bearer " + expected);
    }

    private ObjectNode expectedResponse() throws JsonMappingException, JsonProcessingException {
        return mapper.readValue("{ \"name\": \"value\" }", ObjectNode.class);
    }

    private HttpClient newMockClient(final int statusCode, final ObjectNode response)
            throws IOException, InterruptedException {
        HttpClient mockClient = mock();
        var mockResponse = newMockResponse(statusCode, response);
        when(mockClient.<String>send(notNull(), notNull())).thenReturn(mockResponse);

        return mockClient;
    }

    private HttpResponse<String> newMockResponse(final int statusCode, final ObjectNode response)
            throws JsonProcessingException {
        HttpResponse<String> result = mock();
        when(result.statusCode()).thenReturn(statusCode);
        var responseString = mapper.writeValueAsString(response);
        when(result.body()).thenReturn(responseString);

        return result;
    }

    private Discovery newDiscovery(final HttpClient client) {
        return new Discovery(client, "apiHost");
    }

    private HttpRequest captureRequest(final HttpClient mockClient) throws IOException, InterruptedException {
        var requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockClient).send(requestCaptor.capture(), any());
        return requestCaptor.getValue();
    }
}
