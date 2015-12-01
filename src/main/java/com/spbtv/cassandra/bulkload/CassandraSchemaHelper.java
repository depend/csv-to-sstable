package com.spbtv.cassandra.bulkload;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by depend on 12/1/2015.
 */
public class CassandraSchemaHelper {
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

}
