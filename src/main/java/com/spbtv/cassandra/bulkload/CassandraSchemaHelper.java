package com.spbtv.cassandra.bulkload;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

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
}
