/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

/** An asset defined in Cloud Pak for Data. */
class Asset extends Entity {
    private final Project container;

    Asset(final String key, final String id, final Project container) {
        super(key, id);
        this.container = container;
    }

    /**
     * The Cloud Pak for Data project that defines this asset.
     *
     * @return A project reference.
     */
    public Project container() {
        return container;
    }
}
