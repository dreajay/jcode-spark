package com.jcode.spark;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.Client;
import org.apache.spark.deploy.ClientArguments;
import org.apache.spark.deploy.SparkSubmit;

public class SubmitingSparkApplication {

	public static void main(String[] args) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss"); 
        String filename = dateFormat.format(new Date());
        String tmp=Thread.currentThread().getContextClassLoader().getResource("").getPath();
        tmp =tmp.substring(0, tmp.length()-8);
       
        //Spark standAlone
        String[] argument=new String[]{
                "--master","spark://bd1:7077",
                "--deploy-mode","client",
                "--name","WordCount",
                "--class","com.jcode.spark.WordCount",
                "--executor-memory","1G",
//                "jcode-spark.jar",
//                tmp+"lib/jcode-spark.jar",//
                "hdfs://bd1:9000/user/bd/input/log.txt",
                "hdfs://bd2:9000/user/bd/output/"+filename
        };
         
        SparkSubmit.main(argument);

	}
	 
	    public static void main2(String[] args) {
	        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss"); 
	        String filename = dateFormat.format(new Date());
	        String tmp=Thread.currentThread().getContextClassLoader().getResource("").getPath();
	        tmp =tmp.substring(0, tmp.length()-8);
	        String[] arg0=new String[]{
	                "--name","test java submit job to yarn",
	                "--class","Scala_Test",
	                "--executor-memory","1G",
//	              "WebRoot/WEB-INF/lib/spark_filter.jar",//
	                "--jar",tmp+"lib/spark_filter.jar",//
	                 
	                "--arg","hdfs://node101:8020/user/root/log.txt",
	                "--arg","hdfs://node101:8020/user/root/badLines_yarn_"+filename,
	                "--addJars","hdfs://node101:8020/user/root/servlet-api.jar",//
	                "--archives","hdfs://node101:8020/user/root/servlet-api.jar"//
	        };
	         
//	      SparkSubmit.main(arg0);
	}

}
