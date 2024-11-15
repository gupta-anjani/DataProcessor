package com.hospital.dataProcessor;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.hospital.dataprocessor.HospitalDataProcessor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

class HospitalDataProcessorTest {

    private static SparkSession spark;
    private Dataset<Row> sampleData;

    @BeforeAll
    static void setupClass() {
        // Initialize SparkSession for testing
        spark = SparkSession.builder()
                .appName("Hospital Data Processor Test")
                .master("local[*]") // Local mode for testing
                .getOrCreate();
    }

    @BeforeEach
    void setup() {
    }

    @Test
    void testUpdateColumnsName() {
        // Call the method to be tested
        Dataset<Row> updatedData = HospitalDataProcessor.updateColumnsName(HospitalDataProcessor.loadData(spark));

        // Verify the schema - expected columns after update
        List<String> expectedColumns = Arrays.asList("Name", "Cust_I", "Open_Dt", "Last_Consulted_Date", "DOB", "VAC_ID", "FLAG", "country");
        List<String> actualColumns = Arrays.asList(updatedData.columns());
        
        // Check if the columns are renamed correctly
        assertTrue(actualColumns.containsAll(expectedColumns));
        
        // Verify the data type of the updated columns
        assertEquals("date", updatedData.schema().apply("Open_Dt").dataType().typeName());
        assertEquals("date", updatedData.schema().apply("Last_Consulted_Date").dataType().typeName());
        assertEquals("date", updatedData.schema().apply("DOB").dataType().typeName());
        
        // Check the values of a specific row
        Row firstRow = updatedData.first();
        assertEquals("Kate", firstRow.getAs("Name"));
        assertEquals("123457", firstRow.getAs("Cust_I"));
        assertEquals("2010-10-12", firstRow.getAs("Open_Dt").toString());  // Check the formatted date
        assertEquals("2012-10-13", firstRow.getAs("Last_Consulted_Date").toString());
        assertEquals("1987-03-06", firstRow.getAs("DOB").toString());
        assertEquals("MVD", firstRow.getAs("VAC_ID"));
        assertEquals("A", firstRow.getAs("FLAG"));
    }

    @AfterAll
    static void tearDownClass() {
        // Stop the Spark session after all tests
        spark.stop();
    }
}

