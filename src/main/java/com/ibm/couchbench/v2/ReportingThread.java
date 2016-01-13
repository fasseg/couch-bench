package com.ibm.couchbench.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportingThread implements Runnable {
    private boolean shutdown;
    private final CloudantBench bench;
    private static final Logger log = LoggerFactory.getLogger(ReportingThread.class);

    public ReportingThread(CloudantBench bench) {
        this.bench = bench;
    }


    public void run() {
        long time = bench.getTimeStarted();
        long lastreport = time;
        while (!shutdown) {
            if (System.currentTimeMillis() - lastreport > 10000) {
                float elapsed = (float) (System.currentTimeMillis() - time) / 1000f;
                int numFinished = bench.getNumFinished();
                log.info("{} inserts in {} secs: {} inserts/sec", numFinished, elapsed, (float) numFinished/elapsed);
                lastreport = System.currentTimeMillis();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
    }
}
