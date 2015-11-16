package com.ibm.couchbench;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class InsertThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(InsertThread.class);

    private final long index;
    private final String url;
    private final String payload;

    private long time;

    public InsertThread(long index, String host, int port, String tableName) {
        this.index = index;
        this.url = "http://" + host  + ":" + port + "/" + tableName;
        payload = RandomRecordGenerator.randomRecord();
    }

    public void run() {
        log.debug("inserting record {}", index);
        long start = System.currentTimeMillis();
        insertRecord();
        time = System.currentTimeMillis() - start;
    }

    private void insertRecord() {
        try {
            final HttpResponse resp = Request.Post(url)
                    .bodyString(payload, ContentType.APPLICATION_JSON)
                    .execute()
                    .returnResponse();
        } catch (IOException e) {
            log.error("Error while posting data", e);
        }
    }
}
