/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

/** An entity that can be referenced using the Flight service. */
class Entity {
    private final String key;
    private final String id;

    Entity(final String key, final String id) {
        this.key = key;
        this.id = id;
    }

    /**
     * Key representing the entity type in the Cloud Pak for Data API.
     *
     * @return An asset type key.
     */
    public String key() {
        return key;
    }

    /**
     * ID used to reference the entity in the Cloud Pak for Data API.
     *
     * @return An asset ID.
     */
    public String id() {
        return id;
    }
}
