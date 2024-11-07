/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

/** A data asset defined in Cloud Pak for Data. This is used to access data at a specific location. */
public final class DataAsset extends Asset {

    /**
     * Constructor.
     *
     * @param id The data asset ID.
     * @param container The project that defines this data asset.
     */
    public DataAsset(final String id, final Project container) {
        super("asset_id", id, container);
    }
}
