/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark.flight;

/** A Cloud Pak for Data project reference. */
public final class Project extends Entity {

    /**
     * Constructor.
     *
     * @param id A Project ID.
     */
    public Project(final String id) {
        super("project_id", id);
    }
}
