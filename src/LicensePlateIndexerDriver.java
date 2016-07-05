import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.*;

import org.apache.cassandra.dht.*;

/*This class is responsible for running map reduce job*/
public class LicensePlateIndexerDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception
	{
		if(args.length !=2) {
			System.err.println("Usage: MaxTemperatureDriver <input path> <outputpath>");
			System.exit(-1);
		}
		
		Path outputPath = new Path(args[1]);
		File outputFile = new File(outputPath.toString());
		
		if (outputFile.exists()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
			String archivePath = new String(outputPath.toString() + "_" + sdf.format(Calendar.getInstance().getTime()));
			outputFile.renameTo(new File(archivePath));
		}
		
		// Get our Hadoop Configuration and setup the job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Plate Indexer");
		job.setJarByClass(LicensePlateIndexerDriver.class);
		job.setMapperClass(LicensePlateIndexerMapper.class);
		job.setReducerClass(LicensePlateIndexerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));	// output to file
		
		/*
		Job job = new Job();
		job.setJarByClass(LicensePlateIndexerDriver.class);
		job.setJobName("License Plate Indexer");
		*/
		
		/* Output to cassandra db
		ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "127.0.0.1");
//		ConfigHelper.setOutputKeyspace(job.getConfiguration(), "devkeyspace");
		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "devkeyspace", "plateindex");
//		String query = "insert into plateindex (plate_fragment, plates) values ('?', [?]);";
		String query = "update plateindex set plates = [?];";
		CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
		ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
		
		job.setOutputFormatClass(CqlOutputFormat.class);
		*/
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);
		return success ? 0 : 1;
	}
	
	static char[] ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
	static int PLATE_LENGTH = 6;
	
	private static void buildSourceFile(int rows) {
		int maxIndex = ALPHABET.length;
    	Random alphaIndexGenerator = new Random();

    	try {
        	PrintWriter plateWriter = new PrintWriter("input.txt");
        	
        	for (int idx = 0; idx < rows; idx++) {
    			String plate = new String();
    			for (int jdx = 0; jdx < PLATE_LENGTH; jdx++) {
    				int alphaIndex = alphaIndexGenerator.nextInt(maxIndex);
    				plate += String.valueOf(ALPHABET[alphaIndex]);
    			}
    			plateWriter.println(plate);
    		}
        	plateWriter.close();
    	}
    	catch (FileNotFoundException fnfe) {
    		
    	}
	}
	
	public static void main(String[] args) throws Exception {
		LicensePlateIndexerDriver driver = new LicensePlateIndexerDriver();
		int exitCode = 0;
		if (args.length > 0 && args[0].equals("-n")) {
			buildSourceFile(Integer.parseInt(args[1]));
		}
		else {
			exitCode = ToolRunner.run(driver, args);
		}
		System.exit(exitCode);
	}
}