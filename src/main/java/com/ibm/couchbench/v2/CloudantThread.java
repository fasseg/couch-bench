package com.ibm.couchbench.v2;

import com.ibm.couchbench.RandomRecordGenerator;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

public class CloudantThread implements Runnable {
    private final int inserts;
    private final String url;
    private final Header authHeader;
    private final CloudantBench bench;

    public CloudantThread(CloudantBench bench, String url, Header authHeader, int inserts) {
        this.inserts = inserts;
        this.authHeader = authHeader;
        this.url = url;
        this.bench = bench;
    }


    public void run() {
        for (int i=0;i<inserts;i++) {
           insertRandomRecord();
        }
    }

    private void insertRandomRecord() {
        try {
            final HttpResponse resp = Request.Post(url)
                    .addHeader(authHeader)
                    .bodyString(RandomRecordGenerator.randomRecord(), ContentType.APPLICATION_JSON)
                    .execute()
                    .returnResponse();
            bench.addResponseCode(resp.getStatusLine().getStatusCode());
        } catch (Exception e) {
            bench.addException(e.getClass().getName());
        } finally {
            bench.increaseNumFinished();
        }
    }
}
