/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.http.HttpResponse;

/** Utility functions for dealing with API response messages. */
public final class Responses {

    /**
     * Parse a JSON object response message body.
     *
     * @param response An HTTP response.
     * @return A JSON object.
     * @throws JsonProcessingException if the message body is not a valid JSON object.
     */
    public static ObjectNode parseJsonObject(final HttpResponse<String> response) throws JsonProcessingException {
        var discoveryJson = response.body();
        return new ObjectMapper().readValue(discoveryJson, ObjectNode.class);
    }

    /**
     * Assert that a response message has a valid status code.
     *
     * @param response An HTTP response message.
     * @throws IOException if the response has an invalid status code.
     */
    public static void assertSuccess(final HttpResponse<?> response) throws IOException {
        var code = response.statusCode();
        if (code < 200 || code >= 300) {
            throw new IOException("Unsuccessful HTTP response (" + code + "): " + response.body());
        }
    }

    private Responses() {
        // Private constructor to prevent instantiation
    }
}
