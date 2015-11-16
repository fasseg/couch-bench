package com.ibm.couchbench;

import org.apache.commons.cli.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CouchBench {
    private static final Logger log = LoggerFactory.getLogger(CouchBench.class);

    private final String host;
    private final int port;
    private final long numInserts;
    private final int numThreads;
    private final String tableName;
    private final boolean clean;
    private final List<Runnable> insertQueue = new ArrayList<Runnable>();

    public CouchBench(String host, int port, String tableName, long numInserts, int numThreads, boolean clean) {
        this.host = host;
        this.port = port;
        this.numInserts = numInserts;
        this.numThreads = numThreads;
        this.tableName = tableName;
        this.clean = clean;
    }

    private void runBenchmark() throws IOException {
        log.info("Inserting {} records on {}:{} using {} threads", numInserts, host, port, numThreads);
        initDB();
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (long i = 1; i <= numInserts;i++) {
            insertQueue.add(new InsertThread(i, host, port, tableName));
        }
        long time = System.currentTimeMillis();
        for (final Runnable r : insertQueue) {
            executor.execute(r);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                log.error("Error while waiting for executor shutdown", e);
                e.printStackTrace();
            }
        }
        time = System.currentTimeMillis() - time;
        log.info("Benchmark finished.");
        log.info("{} inserts in {} ms", numInserts, time);
        log.info("Throughput: {} ops/sec", (float) numInserts * 1000f / (float) time );
    }

    private void initDB() throws IOException {
        HttpResponse resp = Request.Get("http://" + host + ":" + port + "/" + tableName)
                .execute()
                .returnResponse();
        if (resp.getStatusLine().getStatusCode() == 404 || (resp.getStatusLine().getStatusCode() == 200 && clean)) {
            if (resp.getStatusLine().getStatusCode() == 200) {
                log.info("cleanin benchmark table");
                resp = Request.Delete("http://" + host + ":" + port + "/" + tableName)
                        .execute()
                        .returnResponse();
                if (resp.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Unable to delete table " + tableName);
                }
            }
            log.info("creating benchmark table");
            resp = Request.Put("http://" + host + ":" + port + "/" + tableName)
                    .execute()
                    .returnResponse();
            if (resp.getStatusLine().getStatusCode() != 201) {
                throw new IOException("Unable to create database table " + tableName);
            }
        }
    }

    public static void main(String[] args) {
        final Options ops = new Options();
        ops.addOption("t", "threads", true, "Number of threads [1]");
        ops.addOption("d", "destination-host", true, "Target host [localhost]");
        ops.addOption("p", "port", true, "Target port [5984]");
        ops.addOption("n", "num-insert", true, "Number of insert operations [1000]");
        ops.addOption("h", "help", false, "Print this help");
        ops.addOption("c", "clean", false, "Drop and recreate the benchmark table");
        ops.addOption("b", "table-name", true, "The database table to use [bench_table]");
        final CouchBench bench = createBench(ops, args);
        try {
            bench.runBenchmark();
        } catch (IOException e) {
            log.error("Error while running benchmark", e);
        }
    }

    private static CouchBench createBench(Options options, String[] args) {

        String host = "localhost";
        int port = 5984;
        int numThreads = 1;
        long numInserts = 1000L;
        boolean clean;
        String tableName = "bench_table";

        final CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            log.error("Exception while parsing command line options", e);
            printHelp(options);
            System.exit(-2);
        }

        if (cmd.hasOption("help")) {
            printHelp(options);
            System.exit(0);
        }

        clean = cmd.hasOption("clean");
        if (cmd.hasOption("table-name")) {
            tableName = cmd.getOptionValue("table-name");
        }
        if (cmd.hasOption("destination-host")) {
            host = cmd.getOptionValue("destination-host");
        }
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("port"));
        }
        if (cmd.hasOption("threads")) {
            numThreads = Integer.parseInt(cmd.getOptionValue("threads"));
        }
        if (cmd.hasOption("num-insert")) {
            numInserts = Long.parseLong(cmd.getOptionValue("num-insert"));
        }
        return new CouchBench(host, port, tableName, numInserts, numThreads, clean);
    }

    private static void printHelp(Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        String footer = "\nExample:\nCouchBench -d 127.0.0.1 -p 5984 -t 5 -n 2000\n";
        formatter.printHelp("CouchBench [options]", null, options, footer);

    }
}
