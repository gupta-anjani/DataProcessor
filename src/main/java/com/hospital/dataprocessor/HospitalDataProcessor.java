package com.hospital.dataprocessor;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class HospitalDataProcessor { 
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Hospital Data Processor")
                .master("local[*]") // Change to your cluster if needed
                .getOrCreate();
        
        spark.sparkContext().setLogLevel("ERROR");
        
        // Load and process data
        Dataset<Row> rawData = loadData(spark);
        //rawData.show();
        Dataset<Row> processedData = addDerivedColumns(rawData);
        Dataset<Row> dataWithUpdatedColumnName = updateColumnsName(processedData);  
        dataWithUpdatedColumnName.show();
        // Validate data
        validateData(dataWithUpdatedColumnName);
        
        // Partition by country and save
        partitionAndSaveData(dataWithUpdatedColumnName);
        // Stop the Spark session
        spark.stop();
    }
    
    public static Dataset<Row> loadData(SparkSession spark) {
        return spark.read()
                .option("header", "true")
                .option("delimiter", "|")
                .option("numPartitions", "50")// decide based on memory, data and core
                .csv("path/to/input/customer_data.csv");
    }
    
    public static Dataset<Row> updateColumnsName(Dataset<Row> data) {
        return data
                .withColumnRenamed("Customer_Name", "Name")
                .withColumnRenamed("Customer_Id", "Cust_I")
                .withColumn("Open_Date", to_date(data.col("Open_Date"), "yyyyMMdd"))
                .withColumn("Last_Consulted_Date", to_date(data.col("Last_Consulted_Date"), "yyyyMMdd"))
                .withColumn("DOB", to_date(data.col("DOB"), "ddMMyyyy"))
                .withColumnRenamed("Open_Date", "Open_Dt")
		        .withColumnRenamed("Vaccination_Id", "VAC_ID")
		        .withColumnRenamed("Is_Active", "FLAG")
		        .drop("_c0", "H");                   
    }
    
    
    public static Dataset<Row> addDerivedColumns(Dataset<Row> data) {
        return data
                .withColumn("age", 
                    datediff(current_date(), to_date(col("DOB"), "ddMMyyyy")).divide(365.25))
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
    
    public static void partitionAndSaveData(Dataset<Row> df) {
    	// Assume the DataFrame has a column named "country"
        List<String> countries = df.select("country").distinct().as(Encoders.STRING()).collectAsList();

        // Iterate over each country and save to the respective table
        for (String country : countries) {
            // Filter the DataFrame by country
            Dataset<Row> countryData = df.filter(df.col("country").equalTo(country));

            // Save the filtered DataFrame to a country-specific table
            countryData.write()
                    .format("jdbc") // or "parquet", "json", etc.
                    .option("url", "jdbc:postgresql://localhost:5432/postgres")
                    .option("dbtable", "table_" + country.toLowerCase()) // e.g., table_usa, table_canada
                    .option("user", "postgres")
                    .option("password", "Hello@2323h")
                    .option("driver", "org.postgresql.Driver") // Specify the PostgreSQL driver
                    .option("numPartitions", "100") // decide based on memory, data and core
                    .mode(SaveMode.Append) // Use SaveMode.Append to add data
                    .save();
        }
    }
}

