package com.snowpark.example;

/*
 * All examples can be found 
 * at https://docs.snowflake.com/en/developer-guide/snowpark/java/working-with-dataframes
 */

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.Column.*;
import java.util.HashMap;
import java.util.Map;

public class joins {
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
    
    //Joining Multiple Tables
    //You can chain the join calls as shown below:

    DataFrame dfFirst = session.table("sample_a");
    DataFrame dfSecond  = session.table("sample_b");
    DataFrame dfThird = session.table("sample_c");
    DataFrame dfJoinThreeTables = dfFirst.join(dfSecond, dfFirst.col("id_a")
        .equal_to(dfSecond.col("id_a")))
        .join(dfThird, dfFirst.col("id_a")
        .equal_to(dfThird.col("id_a")));
    
    dfJoinThreeTables.show();
    


    }
    
}
