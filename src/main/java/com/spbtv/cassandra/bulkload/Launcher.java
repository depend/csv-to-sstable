package com.spbtv.cassandra.bulkload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.prefs.CsvPreference;

import java.util.Map;

/**
 * Created by depend on 12/1/2015.
 */
public class Launcher {
    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    private static final CsvPreference SINGLE_QUOTED_COMMA_DELIMITED = new CsvPreference.Builder(
            '\'', ',', "\n").build();

    public static void main(String[] args) {
        String host = Env.getCassandraHost();
        String username = Env.getCassandraUsername();
        String password = Env.getCassandraPassword();
        String keyspace = Env.getTargetKeyspace();
        String table = Env.getTargetTable();
        String csv_path = Env.getImportInputPath();
        String output_path = Env.getImportOutputPath();

        String schema = Bulkload.getSchema(host, username, password, keyspace, table);
        logger.info(schema);

        Map<String, String> columns = Bulkload.extractColumns(schema.replace("\n", " ").replace("\r", " "));
        logger.info("{} columns", columns.size());

        //Set<String> primaryColumns = extractPrimaryColumns(schema);
    }
}
