package com.amazonaws.utils;

import com.amazonaws.pojo.Location;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to query data from S3Select and consume the response in the form of an
 * InputStream of records and write it to a file.
 */

public class S3LoadData {
    private static final Logger LOG = LoggerFactory.getLogger(S3LoadData.class);
    private static final String CSV_OBJECT_KEY = "location_data.csv";
    private static final String QUERY_PER_ROLE = "SELECT s.location FROM s3object s WHERE s.role = '%s'";
    private static final String QUERY_ALL_OBJECTS = "SELECT role, location FROM s3object";

    private String getLocation(String role) throws Exception {
        //Read S3 bucket name from config
        AppProperties properties = new AppProperties();
        final String bucketName = properties.getProperty("s3.bucket");
        final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        String location = "";

        SelectObjectContentRequest request = generateBaseCSVRequest(bucketName, CSV_OBJECT_KEY, String.format(QUERY_PER_ROLE, role));
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);
        try (SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
            InputStream resultInputStream = result.getPayload().getRecordsInputStream(
                    new SelectObjectContentEventVisitor() {
                        /*
                         * An End Event informs that the request has finished successfully.
                         */
                        @Override
                        public void visit(SelectObjectContentEvent.EndEvent event)
                        {
                            isResultComplete.set(true);
                        }
                    }
            );

            location = new BufferedReader(
                    new InputStreamReader(resultInputStream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

        }

        /*
         * The End Event indicates all matching records have been transmitted.
         * If the End Event is not received, the results may be incomplete.
         */
        if (!isResultComplete.get()) {
            throw new Exception("S3 Select request was incomplete as End Event was not received.");
        }

        return location;
    }

    private Map<String, Location> getLocation() throws Exception {
        //Read S3 bucket name from config
        AppProperties properties = new AppProperties();
        final String bucketName = properties.getProperty("s3.bucket");
        final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        String location = "";
        Map<String, Location> locationMap = new HashMap<>();

        SelectObjectContentRequest request = generateBaseCSVRequest(bucketName, CSV_OBJECT_KEY, QUERY_ALL_OBJECTS);
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);
        try (SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
            InputStream resultInputStream = result.getPayload().getRecordsInputStream(
                    new SelectObjectContentEventVisitor() {
                        /*
                         * An End Event informs that the request has finished successfully.
                         */
                        @Override
                        public void visit(SelectObjectContentEvent.EndEvent event)
                        {
                            isResultComplete.set(true);
                        }
                    }
            );

            location = new BufferedReader(
                    new InputStreamReader(resultInputStream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            List<String> roleLocations = Arrays.stream(location
                            .split("\n"))
                            .collect(Collectors.toList());

            for (String roleLocation: roleLocations) {
                String role = roleLocation.split(",")[0];
                String building = roleLocation.split(",")[1];
                locationMap.put(role, new Location(role, building));
            }
        }

        /*
         * The End Event indicates all matching records have been transmitted.
         * If the End Event is not received, the results may be incomplete.
         */
        if (!isResultComplete.get()) {
            throw new Exception("S3 Select request was incomplete as End Event was not received.");
        }

        return locationMap;
    }

    private SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE));
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(new CSVOutput());
        request.setOutputSerialization(outputSerialization);

        return request;
    }

    public Location loadReferenceLocationData(String key) throws Exception {
        LOG.info("***************************************");
        LOG.info("Loading Location Reference Data for key: " +  key);
        LOG.info("***************************************");

        Location location = new Location(key, getLocation(key));

        return location;
    }

    public Map<String, Location> loadReferenceLocationData() throws Exception {
        LOG.info("***************************************");
        LOG.info("Load Location Reference Data");

        Map<String, Location> map = getLocation();
        LOG.info("Printing map values");
        for (Map.Entry<String, Location> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().getBuildingNo());
        }
        LOG.info("***************************************");

        return map;
    }
}
