package com.spbtv.cassandra.bulkload;

/**
 * Created by depend on 12/1/2015.
 */
public class Launcher {
    public static void main(String[] args) {
        String keyspace = Env.getTargetKeyspace();
        String table = Env.getTargetTable();
        String csv_path = Env.getImportInputPath();
        String output_path = Env.getImportOutputPath();

        
    }
}
