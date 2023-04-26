package com.snowpark.example;

/*
 * More examplese here https://docs.snowflake.com/en/developer-guide/snowpark/java/sql-to-snowpark
 * 
 */
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.Column.*;
import java.util.HashMap;
import java.util.Map;

public class grouping 
{
    public static void main( String[] args )
    {
            // Replace the <placeholders> below.
    Map<String, String> properties = new HashMap<>();
    properties.put("URL", "https://region.account.snowflakecomputing.com:443");
    properties.put("USER", "username");
    properties.put("PASSWORD", "password");
    properties.put("ROLE", "rolename");
    properties.put("WAREHOUSE", "warehouse name");
    properties.put("DB", "DEMO");
    properties.put("SCHEMA", "SNOWPARK_EXAMPLES");
    Session session = Session.builder().configs(properties).create();
    session.sql("select id, name from sample_product_data").show();
    
    //grouping and agg
    DataFrame df = session.table("sample_product_data");
    DataFrame dfCountPerCategory = df.groupBy(Functions.col("category_id")).count();
    dfCountPerCategory.show();
    dfCountPerCategory.select(Functions.col("$1").as("categ"), 
        Functions.col("$2").as("newCount")).show();


    
    //select sum(amount) as "TotalAmount",sum(quantity) as "TotalQuantity",category_id,parent_id from sample_product_data group by category_id,parent_id;
    //https://docs.snowflake.com/en/developer-guide/snowpark/java/working-with-dataframes#chaining-method-calls
    DataFrame dfAgg1 = df.groupBy(Functions.col("category_id"),Functions.col("parent_id"))
        .sum(Functions.col("amount"),Functions.col("quantity"))
        .select(Functions.col("category_id"),Functions.col("parent_id")
        ,Functions.col("$1").as("TotalAmount"), Functions.col("$2").as("TotalQuantity"));
    dfAgg1.show();

    //relational aggregate functions and builtins
    DataFrame dfAgg2 = df.groupBy(Functions.col("category_id"),Functions.col("parent_id"))
    .builtin("max", df.col("amount"),df.col("quantity"))
    .select(Functions.col("category_id"),Functions.col("parent_id"), Functions.col("$3").as("Max_Amount"),Functions.col("$4").as("Max_Quantity"));
    dfAgg2.show();

    }
}