package com.hospital.dataprocessor;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.*;

public class HospitalDataProcessor3 { 
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Hospital Data Processor")
                .master("local[*]") // Change to your cluster if needed
                .getOrCreate();
        
        spark.sparkContext().setLogLevel("ERROR");
        
        // Load and process data
        Dataset<Row> rawData = loadData(spark);
        rawData.show();
        Dataset<Row> processedData = addDerivedColumns(rawData);
        Dataset<Row> dataWithUpdatedColumnName = updateColumnsName(processedData);  
        dataWithUpdatedColumnName.show();
        // Validate data
        validateData(dataWithUpdatedColumnName);
        
        // Partition by country and save
        partitionAndSaveData(dataWithUpdatedColumnName);
    }
    
    public static Dataset<Row> loadData(SparkSession spark) {
        return spark.read()
                .option("header", "true")
                .option("delimiter", "|")
                .csv("path/to/input/customer_data.csv");
    }
    
    public static Dataset<Row> updateColumnsName(Dataset<Row> data) {
        return data
                .withColumnRenamed("Customer_Name", "Name")
                .withColumnRenamed("Customer_Id", "Cust_I")
                .withColumnRenamed("Open_Date", "Open_Dt")
		        .withColumnRenamed("Vaccination_Id", "VAC_ID")
		        .withColumnRenamed("Is_Active", "FLAG")
		        .drop("_c0")
		        .drop("H");

                    
    }
    
    
    public static Dataset<Row> addDerivedColumns(Dataset<Row> data) {
        return data
                .withColumn("age", 
                    datediff(current_date(), to_date(col("DOB"), "yyyyMMdd")).divide(365.25))
                .withColumn("days_since_last_consulted", 
                    datediff(current_date(), to_date(col("Last_Consulted_Date"), "yyyyMMdd")))
                .filter(col("days_since_last_consulted").gt(30));
    }
    
    public static void validateData(Dataset<Row> data) {
        Dataset<Row> invalidData = data.filter(col("Name").isNull()
                                              .or(col("Cust_I").isNull())
                                              .or(col("Open_Dt").isNull()));

        if (invalidData.count() > 0) {
            System.out.println("Invalid data detected:");
            invalidData.show();
        } else {
            System.out.println("Data validation passed.");
        }
    }
    
    public static void partitionAndSaveData(Dataset<Row> data) {
        data.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("Country")
            .parquet("path/to/output/partitioned_data");
    }
}

