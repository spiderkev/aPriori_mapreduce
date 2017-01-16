package apriori_map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;    
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class aprior {
	public static void main(String[] args) {
	//public void getWordCount(String inputFile, String outputFile) throws Exception {
		try {
		Configuration conf = new Configuration();
        String[] otherArgs;
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
        if (otherArgs.length != 2) {
                System.err.println("Usage: aPriori <in> <out>");
                System.exit(2);
        }
        System.err.println(otherArgs[0]);
        System.err.println(otherArgs[1]);
       @SuppressWarnings("deprecation")
       Job job = new Job(conf, "aprior");
       job.setJarByClass(aprior.class); //Tell hadoop the name of the class every cluster has to look for
       
       job.setMapperClass(aprioriMapper.class); //Set class that will be executed by the mapper
       job.setReducerClass(aprioriReducer.class); //Set the class that will be executed as the reducer
      
       job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user
       job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data
      
       FileInputFormat.addInputPath(job,  new Path(otherArgs[0])); //Get input file name
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+ "/w" + Math.random()));
       
       System.exit(job.waitForCompletion(true) ? 0 : 1);
       
       System.err.println("Finish");
		
       BufferedReader bReader = new BufferedReader(new FileReader(otherArgs[0]));
		int count = 0;
		while ((bReader.readLine()) != null) {
			count++;
		}
		System.err.println(count);
		bReader.close();
		
		 try{
  		    PrintWriter writer = new PrintWriter("C:/CIFO_results/apriorioutput.txt", "UTF-8");
  		    writer.println(count);
  		    writer.close();
  		} catch (IOException e) {
  		   // do something
  		}
	} catch (IOException | ClassNotFoundException | InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
}
