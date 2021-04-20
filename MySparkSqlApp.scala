package org.sparkapp.mysparkapp


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MySparkSqlProject {
  
  def main(args: Array[String]): Unit = {
    
    
    val spark = SparkSession.builder()
    .appName("SparkSql_Logic_Scenario")
    .master("local")
    .getOrCreate()
    
    
    val _LOCATION = "hdfs://localhost:9000/Datasets/personal_transactions.csv"
    
    
    val df = spark.read
    .option("header", "true")
    .csv(_LOCATION)
    
    

    println("-------------------------------------- Printing dataframe  -------------------------------------------------")
    //df.show(10)
  
    
    
    println("-------------------------------------- Printing Schema -----------------------------------------------------")
    df.printSchema()
    
    
    /*
          root
           |-- Customer_No: string (nullable = true)
           |-- Card_type: string (nullable = true)
           |-- Date: string (nullable = true)
           |-- Category: string (nullable = true)
           |-- Transaction Type: string (nullable = true)
           |-- Amount: string (nullable = true)
           
    */
    
    
    // to answer any question about date we should cast string to date 
    // for spark 3.0 version and higher you can do this
    
   spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
   val df_cast = df.withColumn("Date",to_date(col("Date"), "MM/dd/yyyy"))
   df_cast.printSchema()
   
       
    /*
          root
           |-- Customer_No: string (nullable = true)
           |-- Card_type: string (nullable = true)
           |-- Date: date (nullable = true)
           |-- Category: string (nullable = true)
           |-- Transaction Type: string (nullable = true)
           |-- Amount: string (nullable = true)
           
    */
   
    df_cast.show(false)
     
   // ---------------------------------- get max amount debited from each customer on latest date ---------------------------//
   
    
    // filter df_cast with Transaction Type == debit
   
    val df_filtered = df_cast.filter(col("Transaction Type") === "debit")
    df_filtered.show(false)
    
    // --------------------------------------  Method One ----------------------------------------------//
    
    // get max or latest date for each customer
    
    val df_maxDate = df_filtered.groupBy("Customer_No").agg(max("Date").alias("Date"))
    df_maxDate.show(false)
    
    
    val df_date = df_filtered.join(df_maxDate , Seq("Customer_No" , "Date") , "inner")
  //  df_date.show(false)
    
    val df_output = df_date.groupBy("Customer_No" , "Date").agg(max("Amount").alias("max_amount"))
    df_output.show()
    
    // -------------------------------------- Method Two -------------------------------------------------//
    
    val df_output2 = df_filtered
    .select("Customer_No" , "Date" ,"Amount")
    .orderBy(desc("Date") , desc("Amount"))
    .dropDuplicates("Customer_No")
    
    
      df_output2.show()
        
      
   // -------------------------------- get total amount for each Category -----------------------------------//
   
    val total_amount_foreach_cat = df_cast.groupBy("Category").agg(sum("Amount").alias("total_amounts"))
   // total_amount_foreach_cat.show()
   
   // --------------------------------- The most amounts Category -------------------------------------------//
   
    val max_amount_cat = total_amount_foreach_cat.orderBy(desc("total_amounts")).limit(1).first
    println(max_amount_cat.toString())

   
    
  }
  
  
}
