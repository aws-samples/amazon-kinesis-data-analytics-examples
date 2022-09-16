package com.amazonaws.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppProperties {
    private InputStream inputStream;
    private static final String PROPERTY_FILE_NAME = "config.properties";
    public String getProperty(String key) throws IOException {
        Properties prop = new Properties();
        try {
            inputStream = getClass().getClassLoader().getResourceAsStream(PROPERTY_FILE_NAME);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + PROPERTY_FILE_NAME + "' not found in the classpath");
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
        return prop.getProperty(key);
    }
}
