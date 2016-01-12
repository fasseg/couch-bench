package com.ibm.couchbench;

import org.apache.commons.cli.*;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class CouchBench {
    private static final Logger log = LoggerFactory.getLogger(CouchBench.class);

    private final String host;
    private final int port;
    private final long numOperations;
    private final int numThreads;
    private final Integer qValue;
    private final String tableName;
    private final boolean clean;
    private final List<Runnable> insertQueue = new ArrayList<Runnable>();
    private AtomicInteger completedInsertCount = new AtomicInteger(0);
    private Executor requestExecutor;

    public CouchBench(String host, int port, String username, String password, String tableName, long numOperations, int numThreads, boolean clean, Integer qValue) {
        this.host = host;
        this.port = port;
        this.numOperations = numOperations;
        this.numThreads = numThreads;
        this.tableName = tableName;
        this.clean = clean;
        this.qValue = qValue;
        final HttpHost httpHost = new HttpHost(host, port);
        requestExecutor = Executor.newInstance();
        if (username != null && password != null) {
            requestExecutor.auth(httpHost, username, password)
                    .authPreemptive(httpHost);
        }
    }

    public Executor getRequestExecutor() {
        return requestExecutor;
    }

    public void incrementInsertCount() {
        completedInsertCount.incrementAndGet();
    }

    private void runBenchmark() throws IOException {
        log.info("Inserting {} records on {}:{} using {} threads", numOperations, host, port, numThreads);
        initDB();
        final ExecutorService threadExecutor = Executors.newFixedThreadPool(numThreads);
        log.info("Preparing {} inserts", numOperations);
        for (long i = 1; i <= numOperations; i++) {
            insertQueue.add(new InsertThread(i, host, port, tableName, this));
        }
        log.info("Sending requests");
        long time = System.currentTimeMillis();
        for (final Runnable r : insertQueue) {
            threadExecutor.execute(r);
        }
        threadExecutor.shutdown();
        long lastInterval = time;
        while (!threadExecutor.isTerminated()) {
            try {
                long interval = System.currentTimeMillis() - lastInterval;
                if (interval > 10000L) {
                    long currentRunTime = System.currentTimeMillis() - time;
                    log.info("Progress: {} inserts in {} seconds: {} inserts/sec", completedInsertCount.get(), (float) (currentRunTime) / 1000f, (float) completedInsertCount.get() * 1000f / (float) currentRunTime);
                    lastInterval = System.currentTimeMillis();
                }
                Thread.sleep(50);
            } catch (InterruptedException e) {
                log.error("Error while waiting for executor shutdown", e);
                e.printStackTrace();
            }
        }
        time = System.currentTimeMillis() - time;
        log.info("Benchmark finished.");
        log.info("{} inserts in {} ms", numOperations, time);
        log.info("Throughput: {} ops/sec", (float) numOperations * 1000f / (float) time );
    }

    private void initDB() throws IOException {
        HttpResponse resp =requestExecutor.execute(Request.Get("http://" + host + ":" + port + "/" + tableName))
                .returnResponse();
        if (resp.getStatusLine().getStatusCode() == 404 || (resp.getStatusLine().getStatusCode() == 200 && clean)) {
            if (resp.getStatusLine().getStatusCode() == 200) {
                log.info("deleting benchmark table");
                resp = requestExecutor.execute(Request.Delete("http://" + host + ":" + port + "/" + tableName))
                        .returnResponse();
                if (resp.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Unable to delete table " + tableName);
                }
            }
            log.info("creating benchmark table");
            String queryString = "";
            if (qValue != null) {
                queryString = "?q=" + qValue;
            }
            resp = requestExecutor.execute(Request.Put("http://" + host + ":" + port + "/" + tableName + queryString))
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
        ops.addOption("l", "port", true, "Target port [5984]");
        ops.addOption("n", "num-operations", true, "Number of operations [1000]");
        ops.addOption("h", "help", false, "Print this help");
        ops.addOption("c", "clean", false, "Drop and recreate the benchmark table");
        ops.addOption("q", "cloudant-q", true, "The Cloudant Quorum Q value to set when creating the table");
        ops.addOption("b", "table-name", true, "The database table to use [bench_table]");
        ops.addOption("u", "user-name", true, "The authentication username");
        ops.addOption("p", "password", true, "The authentication password");
        ops.addOption("q", "cloudant-q", true, "The Cloudant Quorum Q value to set when creating the table");
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
        Integer qValue = null;
        long numInserts = 1000L;
        boolean clean;
        String tableName = "bench_table";
        String username = null;
        String password = null;

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
        if (cmd.hasOption("cloudant-q")) {
            qValue = Integer.parseInt(cmd.getOptionValue("cloudant-q"));
        }
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
        if (cmd.hasOption("num-operations")) {
            numInserts = Long.parseLong(cmd.getOptionValue("num-operations"));
        }
        if (cmd.hasOption("user-name")) {
            username = cmd.getOptionValue("user-name");
        }
        if (cmd.hasOption("password")) {
            password = cmd.getOptionValue("password");
        }
        return new CouchBench(host, port, username, password, tableName, numInserts, numThreads, clean, qValue);
    }

    private static void printHelp(Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        String footer = "\nExample:\nCouchBench -d 127.0.0.1 -p 5984 -t 5 -n 2000\n";
        formatter.printHelp("CouchBench [options]", null, options, footer);

    }
}
