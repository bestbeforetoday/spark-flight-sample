/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

/**
 * A connection defined in Cloud Pak for Data. This defines connection details and credentials required to access data.
 */
public final class Connection extends Asset {
    /**
     * Constructor.
     *
     * @param id The connection ID.
     * @param container The project that defines this connection.
     */
    public Connection(final String id, final Project container) {
        super("connection_id", id, container);
    }
}
