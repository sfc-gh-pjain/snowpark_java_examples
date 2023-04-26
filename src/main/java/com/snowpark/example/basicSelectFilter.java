package com.snowpark.example;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.types.*;
import java.util.HashMap;
import java.util.Map;

public class basicSelectFilter 
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
    

    // Specify the equivalent of "SELECT key * 10 AS c"
    // in an SQL SELECT statement.
    DataFrame df3 = session.table("sample_product_data");
    df3.select(Functions.col("key").multiply(Functions.lit(10)).as("c")).show();

    // Specify the equivalent of "sample_a JOIN sample_b on sample_a.id_a = sample_b.id_a"
    // in an SQL SELECT statement.
    DataFrame dfLhs = session.table("sample_a");
    DataFrame dfRhs = session.table("sample_b");
    DataFrame dfJoined = dfLhs.join(dfRhs, dfLhs.col("id_a").equal_to(dfRhs.col("id_a")));
    dfJoined.show();

    // Create a DataFrame that contains the value 0.05.
    DataFrame df = session.sql("select 0.05 :: Numeric(5, 2) as a");

    // Applying this filter results in no matching rows in the DataFrame. Nothing shows up
    df.filter(Functions.col("a").leq(Functions.lit(0.06).minus(Functions.lit(0.01)))).show();
    //To avoid this problem, cast the literal to the Snowpark type that you want to use.
    df.filter(Functions.col("a").leq(Functions.lit(0.06).cast(DataTypes.createDecimalType(5, 2)).minus(Functions.lit(0.01).cast(DataTypes.createDecimalType(5, 2))))).show();

    //chaining methods
    DataFrame dfProductInfo = session.table("sample_product_data").filter(Functions.col("id").equal_to(Functions.lit(1))).select(Functions.col("name"), Functions.col("serial_number"));
    dfProductInfo.show();

    //filtering data
    DataFrame df4 = session.table("sample_product_data");
    DataFrame dfFilteredRows = df4.where(Functions.col("id").equal_to(Functions.lit(1)));
    dfFilteredRows.show();

    //grouping and agg
    DataFrame df5 = session.table("sample_product_data");
    DataFrame dfCountPerCategory = df5.groupBy(Functions.col("category_id")).count();
    dfCountPerCategory.show();
    

    }

}
