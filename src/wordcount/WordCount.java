package wordcount;
 import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;    
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	public void getWordCount(String inputFile, String outputFile) throws Exception {
		Configuration conf = new Configuration();
		
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class); //Tell hadoop the name of the class every cluster has to look for
        job.setMapperClass(TokenizerMappermap.class); //Set class that will be executed by the mapper
        //job.setCombinerClass(wordCountCombiner.class); //Set class that will be executed as the combiner
        job.setReducerClass(IntSumReducer.class); //Set the class that will be executed as the reducer
        job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user
        job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data
       
        FileInputFormat.addInputPath(job, new Path(inputFile)); //Get input file name
        FileOutputFormat.setOutputPath(job, new Path(outputFile)); //Get output file name
        job.waitForCompletion(true);

	}
}

