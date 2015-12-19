package com.spbtv.cassandra.bulkload;

import com.google.common.base.Joiner;
import com.qsystem.common.util.UnZipHelper;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by depend on 12/1/2015.
 */
public class Launcher {
    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    private static final CsvPreference SINGLE_QUOTED_COMMA_DELIMITED = new CsvPreference.Builder(
            '\'', ',', "\n").build();

    private static void parse(File csvFile) throws IOException, InvalidRequestException {
        String host = Env.getCassandraHost();
        String username = Env.getCassandraUsername();
        String password = Env.getCassandraPassword();
        String keyspace = Env.getTargetKeyspace();
        String table = Env.getTargetTable();
        String output_path = Env.getImportOutputPath();

        String schema = Bulkload.getSchema(host, username, password, keyspace, table);
        logger.info(schema);

        schema = schema.replace("\n", " ").replace("\r", " ");
        Map<String, String> columns = Bulkload.extractColumns(schema);
        logger.info("{} columns", columns.size());

        Set<String> primaryColumns = Bulkload.extractPrimaryColumns(schema);
        logger.info("{} primary keys", primaryColumns.size());

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(output_path + File.separator + keyspace
                + File.separator + table);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: "
                    + outputDir);
        }

        try (
                BufferedReader reader = new BufferedReader(new FileReader(csvFile));
                CsvListReader csvReader = new CsvListReader(reader, CsvPreference.EXCEL_PREFERENCE)) {

            String[] header = csvReader.getHeader(true);

            String insert_stmt = String.format("INSERT INTO %s.%s ("
                    + Joiner.on(", ").join(header)
                    + ") VALUES (" + new String(new char[header.length - 1]).replace("\0", "?, ")
                    + "?)", keyspace, table);

            // Prepare SSTable writer
            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
            // set output directory
            builder.inDirectory(outputDir)
                    // set target schema
                    .forTable(schema)
                    // set CQL statement to put data
                    .using(insert_stmt)
                    // set partitioner if needed
                    // default is Murmur3Partitioner so set if you use different
                    // one.
                    .withPartitioner(new Murmur3Partitioner());
            CQLSSTableWriter writer = builder.build();

            // Write to SSTable while reading data
            List<String> line;
            while ((line = csvReader.read()) != null) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < header.length; i++) {
                    String hdr = header[i].toLowerCase();
                    try {
                        row.put(hdr, Bulkload.parse(line.get(i), columns.get(hdr), primaryColumns.contains(hdr)));
                    } catch (Exception e) {
                        logger.error("fail to parse {}, index: {}, type: {}, value: {}. {}", hdr, i, columns.get(hdr), line.get(i));
                        throw e;
                    }
                }
                try {
                    writer.addRow(row);
                } catch (InvalidRequestException e) {
                    logger.warn("fail to add row.", e);
                    logger.warn("row: {}", Joiner.on(", ").useForNull("NULL").join(line));
                }

            }

            writer.close();

        }
    }

    private static Path createTmpDir() throws IOException {
        String tmpRoot = System.getProperty("java.io.tmpdir");
        File dir = File.createTempFile("option", "", new File(tmpRoot));
        dir.delete();
        dir.mkdir();
        return dir.toPath();
    }

    private static void archive(File zipFile) throws IOException {
        logger.info("archiving {}", zipFile);
        Path moveTarget = Paths.get(Env.getImportInputPath(), "processed");
        File target = new File(moveTarget.toFile(), zipFile.getName());

        logger.info("move to {}", target.toPath());
        if (!Files.exists(target.toPath()))
            FileUtils.moveFileToDirectory(zipFile, moveTarget.toFile(), true);
        else {
            logger.info("target exists.");
            for (File f : moveTarget.toFile().listFiles()) {
                logger.info("{}", f.getPath());
            }
        }
    }

    private static void processZip(File zipFile) throws IOException, InvalidRequestException {
        if (zipFile.isDirectory()) {
            return;
        }

        Path unzipPath = createTmpDir();

        try {
            logger.info("unzip {}", zipFile);
            UnZipHelper.unZip(zipFile.toPath(), unzipPath);
            for (File f : unzipPath.toFile().listFiles()) {
                parse(f);
            }

            archive(zipFile);
            logger.info("finish processing {}", zipFile);
        } finally {
            logger.info("cleanup unzip folder {} ...", unzipPath);
            FileUtils.deleteDirectory(unzipPath.toFile());
        }
    }

    public static void main(String[] args) {
        try {
            String input_path = Env.getImportInputPath();
            File zipPath = new File(input_path);

            for (File f : zipPath.listFiles()) {
                processZip(f);
            }
            logger.info("Done.");
        } catch (Exception e) {
            logger.error("fail to load.", e);
        }
    }
}
