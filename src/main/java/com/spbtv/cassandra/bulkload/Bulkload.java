package com.spbtv.cassandra.bulkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.qsystem.common.util.TimestampHelper;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import com.google.common.base.Joiner;

/**
 * Usage: java bulkload.BulkLoad <keyspace> <absolute/path/to/schema.cql> <absolute/path/to/input.csv> <absolute/path/to/output/dir> [optional csv prefs in JSON - default is "{\"col_sep\":\",\", \"quote_char\":\"'\"}" ]
 */
public class Bulkload {

    private static final CsvPreference SINGLE_QUOTED_COMMA_DELIMITED = new CsvPreference.Builder(
            '\'', ',', "\n").build();

    private static String readFile(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static String getSchema(String host, String username, String password, String keyspace, String table) {
        Cluster.Builder builder = Cluster.builder()
                .withCredentials(username, password);

        String[] hosts = host.split(";");
        for (String h : hosts) {
            builder.addContactPoint(h);
        }
        Cluster cluster = builder.build();
        KeyspaceMetadata km = cluster.getMetadata().getKeyspace(keyspace);
        TableMetadata tm = km.getTable(table);
        return tm.exportAsString();
    }

    public static Map<String, String> extractColumns(String schema) {
        Map<String, String> cols = new HashMap<>();
        Pattern columnsPattern = Pattern.compile(".*?\\((.*?)(?:,\\s*PRIMARY KEY.*)?\\).*");
        Matcher m = columnsPattern.matcher(schema);
        if (m.matches()) {
            for (String col : m.group(1).split(",")) {
                String[] name_type_prim = col.trim().split("\\s+");
                if (name_type_prim.length <= 4 && !name_type_prim[0].toUpperCase().equals("PRIMARY"))
                    cols.put(name_type_prim[0], name_type_prim[1]);
            }

        } else throw new RuntimeException("Could not extract columns from provided schema.");
        return cols;
    }

    public static Set<String> extractPrimaryColumns(String schema) {
        Set<String> primary = new HashSet<>();

        // Primary key defined on the same line as the corresponding column
        Pattern pattern = Pattern.compile(".*?(\\w+)\\s+\\w+\\s+PRIMARY KEY.*");
        Matcher m = pattern.matcher(schema);
        if (m.matches()) {
            primary.add(m.group(1));
            return primary;
        }

        // Multi-columns primary key defined on a separate line
        pattern = Pattern.compile(".*PRIMARY KEY\\s*\\(\\s*\\((.*?)\\).*\\).*");
        m = pattern.matcher(schema);
        if (m.matches()) {
            for (String col : m.group(1).split(",")) {
                primary.add(col.trim());
            }
            return primary;
        }

        // Single-column primary key defined on a separate line
        pattern = Pattern.compile(".*PRIMARY KEY\\s*\\(\\s*\\(?\\s*(\\w+)\\s*\\)?,?.*\\).*");
        m = pattern.matcher(schema);
        if (m.matches()) {
            primary.add(m.group(1));
            return primary;
        }


        throw new RuntimeException("Could not extract primary columns from provided schema.");
    }

    private static String extractTable(String schema, String keyspace) {
        Pattern columnsPattern = Pattern.compile(".*\\s+" + keyspace + "\\.(\\w+)\\s*\\(.*");
        Matcher m = columnsPattern.matcher(schema);
        if (m.matches()) {
            return m.group(1).trim();
        }
        throw new RuntimeException("Could not extract table name from provided schema.");
    }

    public static Object parseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (Exception e) {
            return TimestampHelper.dateStringToLong(value, "MM/dd/yyyy", DateTimeZone.UTC);
        }
    }

    public static Object parse(String value, String type, boolean columnIsPrimary) {
        // We use Java types here based on
        // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29

        if (value == null) {
            if (columnIsPrimary) {
                if (type.toLowerCase().equals("text")) return "";
                else throw new RuntimeException("A primary column of type " + type + " was null.");
            }
            return null;
        }
        switch (type.toLowerCase()) {
            case "text":
                return value;
            case "float":
                return Float.parseFloat(value);
            case "int":
                return Integer.parseInt(value);
            case "boolean":
                return Boolean.parseBoolean(value);
            case "double":
                return Double.parseDouble(value);
            case "bigint":
                return parseLong(value);
            case "set<text>":
                JSONParser parser = new JSONParser();
                try {
                    JSONArray json_list = (JSONArray) parser.parse(value);
                    Set<String> set = new HashSet<String>();
                    for (int i = 0; i < json_list.size(); i++) {
                        set.add(json_list.get(i).toString());
                    }
                    return set;
                } catch (ParseException e) {
                    throw new RuntimeException("Cannot parse provided set<text> column. Got " + value + ".");
                }
            default:
                throw new RuntimeException("Cannot parse type '" + type + "'.");
        }
    }

    private static CsvPreference parseCsvPrefs(String prefs) throws Exception {
        JSONParser parser = new JSONParser();
        JSONObject hash = (JSONObject) parser.parse(prefs);
        char col_sep = ((String) hash.get("col_sep")).charAt(0);
        char quote_char = ((String) hash.get("quote_char")).charAt(0);
        return new CsvPreference.Builder(quote_char, col_sep, "\n").build();
    }
}