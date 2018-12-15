package edu.ucr.cs.cs226.spith001;
import org.apache.spark.sql.Encoders;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;

public class Knnsql {
	static Integer k;
    static String inputfile;
    //list_q as POJO class
	public static class list_q implements Serializable {
		private Double distance;// class which initializes distance and points value
	    private String id;
	    public Double getDistance() {
	      return distance;
	    }
	    public void setDistance(Double distance) {
	      this.distance = distance;
	    }
	    public String getId() {
	      return id;
	    }
	    public void setId(String id) {
	      this.id = id;
	    }
	  }
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession //creating sparksession
	            .builder()
	            .appName("knnsql")
	            .getOrCreate();
		k = Integer.parseInt(args[3]);// k value
		//String q=args[1]; //query point
		//String[] querypoint = q.split(",");
		final double x=Double.parseDouble(args[1]);// x value
		final double y =Double.parseDouble(args[2]);// y value
		String inputfile = args[0];// input file name
		JavaRDD<list_q> listRDD = spark.read()// txt to RDD
		    	      .textFile(inputfile)
		    	      .javaRDD()
		    	      .map(new Function<String,list_q>() {
		@Override
		public list_q call(String line) throws Exception {
			String[] tokens = line.split(","); 
			Double distance =((Math.sqrt(Math.pow((x-Double.parseDouble(tokens[1])),2) + Math.pow((y-Double.parseDouble(tokens[2])),2))));
			list_q query = new list_q();
			query.setDistance(distance);// distance value 
			query.setId(line);//
			return query;
			 }
		});
		Dataset<Row> knnlist = spark.createDataFrame(listRDD, list_q.class);//creating dataframe from rdd reference and storing it in dataset
		knnlist.createOrReplaceTempView("query");
		Dataset<Row> knnsql = knnlist.orderBy(org.apache.spark.sql.functions.col("distance").asc());
		knnsql = knnsql.limit(k);
		//displaying the top k values 
		knnsql.write().format("csv").save(inputfile + "sparkoutput10");
		spark.stop();          
		           
	}

	

}
