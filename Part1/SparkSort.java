import java.io.*;
import java.util.Map;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import java.lang.*;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import scala.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;             
import org.apache.spark.api.java.function.Function2;


public class SparkSort {

	public static void main(String[] args) {
		//Creating RDD
		SparkConf conf = new SparkConf().setAppName("sort");
		//translation on record RDD
		JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> k10 = record.map( new function_k() ); 
		JavaRDD<String> record = sc.textFile(args[0]);
//JavaRDD<String> k10 = record.map( new function_k() ); 
		class function_k implements Function<String , String> 
			{
			public String call(String x) {return x.substring(0,10);}
			//JavaRDD<String> k10 = record.map( new function_k() ); 
			}
	//end of map key function

		class function_v implements Function<String,String>
			{
			public String call(String x) {return x.substring(12,98);}
			//JavaRDD<String> k10 = record.map( new function_k() ); 
			}//end of map value function
		class mapper_kv implements Function<String,Tuple2<String, String>>
			{
			public Tuple2<String, String> call(String x) {return new Tuple2(x.substring(0,10),x.substring(12,98));}
			//JavaPairRDD<String, String> record_kv = k10.mapToPair(new mapper_kv());
			}
		PairFunction<String, String, String> keyData= 
				new PairFunction<String, String, String>() {
					//JavaPairRDD<String, String> record_kv = k10.mapToPair(new mapper_kv());
			public Tuple2<String, String> call(String x) {return new Tuple2(x.substring(0,10),x.substring(12,98));}
		};
		//translation on record RDD
		
		//JavaRDD<String> v90 = record.map( new function_v() ); 

		//JavaPairRDD<String, Integer> rec=k10.zip(v90);
		JavaPairRDD<String, String> rec= record.mapToPair(keyData);

		//JavaPairRDD<String, String> record_kv = k10.mapToPair(new mapper_kv());

		rec.sortByKey(true).saveAsTextFile(args[1]);
		
	}//end of main method

} //end of sorting class