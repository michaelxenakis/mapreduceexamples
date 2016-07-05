import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LicensePlateIndexerMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;
	
	@Override
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Inputs will be "license plates"
		// Reduce for each character sequence
		
		String line = value.toString();
		for (int idx = 0; idx < (line.length() - 3); idx++) {
			Text newKey = new Text(line.substring(idx, idx+4));
//			System.out.println("New Key = " + newKey);
			context.write(newKey, new Text(value));
		}
	}
}