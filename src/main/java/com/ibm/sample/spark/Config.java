/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 
package com.ibm.sample.spark;

import com.ibm.sample.spark.flight.Connection;
import com.ibm.sample.spark.flight.DataAsset;
import com.ibm.sample.spark.flight.Project;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/** Configuration for the application. Configuration properties are loaded from {@code resources/config.properties}. */
public final class Config {
    private static final Config INSTANCE = new Config();
    private static final String RESOURCE_NAME = "config.properties";
    private static final String AUTH_KEY_ENV = "AUTH_KEY";

    private final Properties configProps;
    private final Project project;
    private final DataAsset nameAsset;
    private final DataAsset numeralAsset;
    private final Connection connection;
    private final String joinColumnName;
    private final String resultPath;
    private final URI authEndpoint;
    private final String apiHost;

    /**
     * Get the singleton configuration instance.
     *
     * @return Configuration.
     */
    public static Config getInstance() {
        return INSTANCE;
    }

    // Private constructor to prevent instantiation
    private Config() {
        configProps = load();
        project = new Project(getProperty("project"));
        nameAsset = new DataAsset(getProperty("name_asset"), project);
        numeralAsset = new DataAsset(getProperty("numeral_asset"), project);
        connection = new Connection(getProperty("connection"), project);
        joinColumnName = getProperty("join_column_name");
        resultPath = getProperty("result_path");
        authEndpoint = URI.create(getProperty("auth_endpoint"));
        apiHost = getProperty("api_host");
    }

    private Properties load() {
        try (var configStream = getClass().getClassLoader().getResourceAsStream(RESOURCE_NAME);
                var configReader = new InputStreamReader(configStream, StandardCharsets.UTF_8)) {
            var config = new Properties();
            config.load(configReader);
            return config;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getProperty(final String key) {
        var result = configProps.getProperty(key);
        if (null == result) {
            throw new IllegalStateException("Configuration property \"" + key + "\" is not defined");
        }

        return result;
    }

    /**
     * API key used to interact with the authorization service.
     *
     * @return And API key.
     */
    public String getAuthKey() {
        return envOrThrow(AUTH_KEY_ENV);
    }

    private String envOrThrow(final String key) {
        var result = System.getenv(key);
        if (null == result) {
            throw new IllegalStateException("Required environment variable \"" + key + "\" is not set");
        }

        return result;
    }

    /**
     * Reference to the Cloud Pak for Data project where data assets and connections are defined.
     *
     * @return A project reference.
     */
    public Project getProject() {
        return project;
    }

    /**
     * Reference to the Name input table data.
     *
     * @return A data asset reference.
     */
    public DataAsset getNameAsset() {
        return nameAsset;
    }

    /**
     * Reference to the Numeral input table data.
     *
     * @return A data asset reference.
     */
    public DataAsset getNumeralAsset() {
        return numeralAsset;
    }

    /**
     * Reference to the Cloud Object Storage connection used to write the output table data.
     *
     * @return A connection reference.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Name of the column used to join the input tables.
     *
     * @return A column name.
     */
    public String getJoinColumnName() {
        return joinColumnName;
    }

    /**
     * Path within Cloud Object Storage where the output table is to be written. The path includes the bucket name.
     *
     * @return A path name.
     */
    public String getResultPath() {
        return resultPath;
    }

    /**
     * Authorization API endpoint address.
     *
     * @return An endpoint URI.
     */
    public URI getAuthEndpoint() {
        return authEndpoint;
    }

    /**
     * Flight service API host name.
     *
     * @return A host name.
     */
    public String getApiHost() {
        return apiHost;
    }
}
