package com.ibm.couchbench;

import org.apache.commons.lang3.RandomStringUtils;

public class RandomRecordGenerator {
    public static String randomRecord() {
        final StringBuilder record = new StringBuilder();
        record.append("{");
        for (int i = 1; i <= 10; i++) {
            record.append("\"field")
                    .append(i)
                    .append("\" : \"")
                    .append(RandomStringUtils.randomAlphanumeric(100))
                    .append("\"");
            if (i < 10) {
                record.append(",");
            }
        }
        record.append("}");
        return record.toString();
    }
}
