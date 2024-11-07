/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark;

import com.ibm.sample.cloud.Authentication;
import com.ibm.sample.spark.flight.DataAsset;
import com.ibm.sample.spark.flight.Discovery;
import com.ibm.sample.spark.flight.OptionsBuilder;
import java.io.IOException;
import java.net.http.HttpClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Defines the Spark application logic, and a {@link #main(String[])} method that acts as an entry point when executed
 * within Spark.
 */
public final class App {
    /** Spark format string to select Arrow Flight as the underlying data source. */
    private static final String FLIGHT_FORMAT = "com.ibm.connect.spark.flight";

    /**
     * The number of rows to read from the source per chunk. Setting a low batch size can decrease performance by
     * reading from the source too often.
     */
    private static final int BATCH_SIZE = 10_000;

    /**
     * Defines how many endpoints the Flight service is expected to provide to the client.
     *
     * <p>Some data sources can provide access to data by using multiple endpoints. This technique can be used to read
     * multiple streams in parallel threads. The value is ignored if the data source does not support multiple
     * endpoints.
     */
    private static final int NUM_PARTITIONS = 2;

    private static final String TIMEOUT = "60s";

    private static final Config CONFIG = Config.getInstance();

    private final SparkSession session;
    private final String accessToken;
    private final Discovery discovery;

    /**
     * Application execution starts here.
     *
     * @param args command-line arguments.
     * @throws IOException if an error occurs invoking remote service.
     * @throws InterruptedException if execution is interrupted unexpectedly.
     */
    public static void main(final String[] args) throws IOException, InterruptedException {
        try (var session = SparkSession.builder().appName("Sample join").getOrCreate()) {
            // HTTP client used to interact with the authentication and Flight discovery REST APIs.
            var client = HttpClient.newHttpClient();

            // Obtain an access token used to interact with the Flight service.
            var auth = new Authentication(client, CONFIG.getAuthEndpoint());
            var accessToken = auth.accessToken(CONFIG.getAuthKey());

            // Create and run the Spark application.
            new App(session, client, accessToken).run();
        }
    }

    /**
     * Constructor.
     *
     * @param session Spark session within which the application code is executed.
     * @param client HTTP client used to interact with the Flight service.
     * @param accessToken Access token used to access the Flight service.
     */
    App(final SparkSession session, final HttpClient client, final String accessToken) {
        this.session = session;
        this.accessToken = accessToken;

        discovery = new Discovery(client, CONFIG.getApiHost());
        discovery.setAccessToken(accessToken);
    }

    /**
     * Execute the application logic.
     *
     * @throws IOException if a failure occurs reading or writing data.
     * @throws InterruptedException if execution is interrupted unexpectedly.
     */
    public void run() throws IOException, InterruptedException {
        var names = readData(CONFIG.getNameAsset());
        var numerals = readData(CONFIG.getNumeralAsset());

        var results = names.join(numerals, CONFIG.getJoinColumnName());

        writeData(results, CONFIG.getResultPath());
    }

    /**
     * Read a Spark dataset from a data asset using the Flight service.
     *
     * @param asset A data asset definition discovered using Flight and referring to the data.
     * @return a dataset.
     * @throws IOException if a failure occurs reading data.
     * @throws InterruptedException if execution is interrupted unexpectedly.
     */
    private Dataset<Row> readData(final DataAsset asset) throws IOException, InterruptedException {
        var discovery = this.discovery.discover(asset);
        session.log().atInfo().log(
                ">>> Asset discovery = " + discovery.getFlightData().toPrettyString());
        var options = defaultOptions(discovery).numPartitions(NUM_PARTITIONS).build();

        return session.read().format(FLIGHT_FORMAT).options(options).load();
    }

    /**
     * Write a Spark dataset to a path relative to a connection using the Flight service.
     *
     * @param dataset A dataset to be written.
     * @param path Path relative to a connection definition discovered using Flight.
     * @throws IOException if a failure occurs writing data.
     * @throws InterruptedException if execution is interrupted unexpectedly.
     */
    private void writeData(final Dataset<Row> dataset, final String path) throws IOException, InterruptedException {
        var discoveryResult = this.discovery.discover(CONFIG.getConnection(), path);
        session.log().atInfo().log(">>> Connection/path discovery = "
                + discoveryResult.getFlightData().toPrettyString());
        var options = defaultOptions(discoveryResult).build();

        dataset.write()
                .format(FLIGHT_FORMAT)
                .options(options)
                .mode(SaveMode.Overwrite)
                .save();
    }

    /**
     * Set common Flight options used by both read and write operations.
     *
     * @param discovery Flight discovery results.
     * @return A configured Flight options builder.
     */
    private OptionsBuilder defaultOptions(final Discovery.Result discovery) {
        return discovery
                .options()
                .accessToken(accessToken)
                .batchSize(BATCH_SIZE)
                .timeout(TIMEOUT);
    }
}
