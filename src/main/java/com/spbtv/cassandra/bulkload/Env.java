package com.spbtv.cassandra.bulkload;

import com.qsystem.common.util.EnvHelper;

/**
 * Created by depend on 12/1/2015.
 */
public class Env {
    public static String getCassandraHost() {
        return EnvHelper.getAsString("QS_CASSANDRA_HOST");
    }

    public static String getCassandraUsername() {
        return EnvHelper.getAsString("QS_CASSANDRA_USERNAME");
    }

    public static String getCassandraPassword() {
        return EnvHelper.getAsString("QS_CASSANDRA_PASSWORD");
    }

    public static String getTargetKeyspace() {
        return EnvHelper.getAsString("QS_IMPORT_KEYSPACE");
    }

    public static String getTargetTable() {
        return EnvHelper.getAsString("QS_IMPORT_TABLE");
    }

    public static String getImportInputPath() {
        return EnvHelper.getAsString("QS_IMPORT_INPUT_PATH");
    }

    public static String getImportOutputPath() {
        return EnvHelper.getAsString("QS_IMPORT_OUTPUT_PATH");
    }
}
