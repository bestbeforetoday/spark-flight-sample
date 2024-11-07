/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

/** Context parameter value for Flight discovery requests. */
enum Context {
    /** Used when discovering data assets. */
    SOURCE("source"),
    /** Used when discovering paths relative to connections. */
    TARGET("target");

    private final String value;

    Context(final String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
