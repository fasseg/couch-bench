package com.ibm.couchbench.v2;

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
    private int numThreads = 200;
    private int numInsertsPerThread = 5000;
    private AtomicInteger numFinished = new AtomicInteger(0);
    private String host = "10.1.9.170";
    private int port = 80;
    private String user = "admin";
    private String pass = "pass";
    private String tableName = "footable";
    private long timeStarted;
    private final Map<Integer, Integer> responseCodes = new HashMap<Integer,Integer>();
    private final Map<String, Integer> exceptions = new HashMap<String, Integer>();

    public int getNumFinished() {
        return numFinished.get();
    }

    public Map<Integer, Integer> getResponseCodes() {
        return responseCodes;
    }

    public Map<String, Integer> getExceptions() {
        return exceptions;
    }

    public static void main(String[] args) {
        CloudantBench bench = new CloudantBench();
        bench.runBenchmark();
    }

    public void increaseNumFinished() {
        numFinished.incrementAndGet();
    }

    public long getTimeStarted() {
        return timeStarted;
    }

    private void runBenchmark() {
        log.info("Starting {} threads with {} inserts each", numThreads, numInsertsPerThread);
        final List<Thread> threads = new ArrayList<Thread>();
        final Header authHeader = new BasicHeader("Authorization", "Basic " + Base64.encodeBase64String(new String(user + ":" + pass).getBytes()));
        timeStarted = System.currentTimeMillis();
        ReportingThread reporter = new ReportingThread(this);
        new Thread(reporter).start();
        for (int i = 0; i< numThreads; i++) {
            final CloudantThread t = new CloudantThread(this, "http://" + host + ":" + port + "/" + tableName, authHeader, numInsertsPerThread);
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
        log.info("finished {} inserts in {} seconds: {} inserts/sec", numFinished.get(), secs, (float) numFinished.get()/secs);
        for (Map.Entry<Integer, Integer> entry : responseCodes.entrySet()) {
            log.info(entry.getKey() + ": " + entry.getValue());
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
            }else {
                exceptions.put(name, 1);
            }
        }
    }
}
