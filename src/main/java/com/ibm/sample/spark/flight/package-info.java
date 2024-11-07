/**
 * Flight service API, used to discover information about assets and generate Spark options for interacting with them
 * from Spark flows using the Flight service.
 *
 * <p>To interact with data using the Flight service:
 *
 * <ol>
 *   <li>Use the {@link com.ibm.sample.spark.flight.Discovery} class is to locate a defined
 *       {@link com.ibm.sample.spark.flight.DataAsset}, or asset paths relative to a defined
 *       {@link com.ibm.sample.spark.flight.Connection}.
 *   <li>Build Spark options from the discovery results, which can be used with Spark Dataset read and write operations.
 * </ol>
 */
package com.ibm.sample.spark.flight;
