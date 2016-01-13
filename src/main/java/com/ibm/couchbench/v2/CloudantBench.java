package com.ibm.couchbench.v2;

import org.apache.commons.cli.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudantBench {

    private static final Logger log = LoggerFactory.getLogger(CloudantBench.class);

    private final String host;
    private final int numThreads;
    private final int numInsertsPerThread;
    private final String user;
    private final String pass;
    private final int port;

    private final String tableName;
    private AtomicInteger numFinished = new AtomicInteger(0);
    private long timeStarted;
    private final Map<Integer, Integer> responseCodes = new HashMap<Integer, Integer>();
    private final Map<String, Integer> exceptions = new HashMap<String, Integer>();

    public CloudantBench(String host, int port, int numThreads, int numInsertsPerThread, String user, String pass, String tableName) {
        this.host = host;
        this.numThreads = numThreads;
        this.numInsertsPerThread = numInsertsPerThread;
        this.user = user;
        this.pass = pass;
        this.tableName = tableName;
        this.port = port;
    }

    public static void main(String[] args) {
        String host = "localhost";
        int port = 80;
        int numThreads = 1;
        int numInsertsPerThread = 1000;
        String user = null;
        String pass = null;
        String tableName = "cbench";

        Options ops = new Options();
        ops.addOption("d", "destination-host", true, "The target host [" + host + "]");
        ops.addOption("l", "port", true, "The target port [" + port + "]");
        ops.addOption("t", "num-threads", true, "The number of threads [" + numThreads + "]");
        ops.addOption("n", "num-inserts", true, "The number of inserts per thread [" + numInsertsPerThread + "]");
        ops.addOption("a", "table-name", true, "The name of the table to use [" + tableName + "]");
        ops.addOption("u", "user", true, "The username used to log into Cloudant");
        ops.addOption("p", "pass", true, "The user's password");
        ops.addOption("h", "help", false, "This help dialog");

        try {
            CommandLine cmd = new DefaultParser().parse(ops, args);
            if (cmd.hasOption("help")) {
                printHelp(ops);
                System.exit(0);
            }
            if (cmd.hasOption("destination-host")) {
                host = cmd.getOptionValue("destination-host");
            }
            if (cmd.hasOption("port")) {
                port = Integer.parseInt(cmd.getOptionValue("port"));
            }
            if (cmd.hasOption("num-threads")) {
                numThreads = Integer.parseInt(cmd.getOptionValue("num-threads"));
            }
            if (cmd.hasOption("num-inserts")) {
                numInsertsPerThread = Integer.parseInt(cmd.getOptionValue("num-inserts"));
            }
            if (cmd.hasOption("table-name")) {
                tableName = cmd.getOptionValue("table-name");
            }
            if (cmd.hasOption("user")) {
                user = cmd.getOptionValue("user");
            }
            if (cmd.hasOption("pass")) {
                pass = cmd.getOptionValue("pass");
            }

            CloudantBench bench = new CloudantBench(host, port, numThreads, numInsertsPerThread, user, pass, tableName);
            bench.runBenchmark();
        } catch (ParseException e) {
            e.printStackTrace();
            printHelp(ops);
        }


    }

    private static void printHelp(Options ops) {
        new HelpFormatter().printHelp("CouchBench <options>", ops);
    }

    public int getNumFinished() {
        return numFinished.get();
    }

    public Map<Integer, Integer> getResponseCodes() {
        return responseCodes;
    }

    public Map<String, Integer> getExceptions() {
        return exceptions;
    }

    public void increaseNumFinished() {
        numFinished.incrementAndGet();
    }

    public long getTimeStarted() {
        return timeStarted;
    }

    private void runBenchmark() {
        final List<Thread> threads = new ArrayList<Thread>();
        final Header authHeader;
        if (user != null && pass != null) {
            authHeader = new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String(new String(user + ":" + pass).getBytes()));
        } else {
            authHeader = null;
        }
        timeStarted = System.currentTimeMillis();
        ReportingThread reporter = new ReportingThread(this);
        new Thread(reporter).start();
        String url = "http://" + host + ":" + port + "/" + tableName;
        log.info("Initiating {} thread[s] with {} insert[s] each at {}", numThreads, numInsertsPerThread, url);
        for (int i = 0; i < numThreads; i++) {
            final CloudantThread t = new CloudantThread(this, url, authHeader, numInsertsPerThread);
            final Thread thread = new Thread(t);
            thread.start();
            threads.add(thread);
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        reporter.setShutdown(true);
        float secs = (float) (System.currentTimeMillis() - timeStarted) / 1000f;
        log.info("finished {} inserts in {} seconds: {} inserts/sec", numFinished.get(), secs, (float) numFinished.get() / secs);
        if (responseCodes.size() > 0) {
            log.info("-------------Response codes--------------");
        }
        for (Map.Entry<Integer, Integer> entry : responseCodes.entrySet()) {
            log.info(entry.getKey() + ": " + entry.getValue());
        }
        if (exceptions.size() > 0) {
            log.info("---------------Exceptions----------------");
        }
        for (Map.Entry<String, Integer> entry : exceptions.entrySet()) {
            log.info(entry.getKey() + ": " + entry.getValue());
        }
    }

    public void addResponseCode(int statusCode) {
        synchronized (responseCodes) {
            if (responseCodes.containsKey(statusCode)) {
                responseCodes.put(statusCode, responseCodes.get(statusCode) + 1);
            } else {
                responseCodes.put(statusCode, 1);
            }
        }
    }

    public void addException(String name) {
        synchronized (exceptions) {
            if (exceptions.containsKey(name)) {
                exceptions.put(name, exceptions.get(name) + 1);
            } else {
                exceptions.put(name, 1);
            }
        }
    }
}
