import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;

// Class definition for writing to Cassandra
// public class LicensePlateIndexerReducer extends Reducer<Text, Text, MapWritable, ArrayList> {

// Class definition for writing to file
public class LicensePlateIndexerReducer extends Reducer<Text, Text, Text, TextArrayWritable> {
	
	private Cluster cassandraCluster;
	private Session cassandraSession;
	
    //Cassandra helper used to write data
    private CassandraHelper cclient = new CassandraHelper();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<Text> valueList = new ArrayList<Text>();
		
		System.out.println("Starting reduce..." + this.toString());
		
		for (Text value : values) {
			if (!valueList.contains(value)) {
				valueList.add(new Text(value));
			}
		}

		String[] stringList = new String[valueList.size()];
		String valuesStatement = "";
		for (int idx = 0; idx < valueList.size(); idx++) {
			stringList[idx] = valueList.get(idx).toString();
			if (idx == 0) {
				valuesStatement = "'" + valueList.get(idx).toString() + "'";
			}
			else {
				valuesStatement += ", '" + valueList.get(idx).toString() + "'";
			}
		}
		
//		cassandraCluster = Cluster.builder().addContactPoint("127.0.0.1").build();
//		cassandraSession = cassandraCluster.connect("devkeyspace");
		
//		String insertStatement = "insert into plateindex (plate_fragment, plates) values ('" + key + "', [" + valuesStatement + "]);";
//		cassandraSession.execute(insertStatement);
		
//		cassandraSession.close();
//		cassandraCluster.close();

		/*
		PreparedStatement statement = cassandraSession.prepare("insert into plateindex (plate_fragment, plates) values (?, [?]);");
		BoundStatement boundStatement = new BoundStatement(statement);
		cassandraSession.execute(boundStatement.bind(key, valuesStatement));*/
		
		context.write(key, new TextArrayWritable(stringList));
		cclient.addKeyValues(key.toString(), stringList);
		/*
		MapWritable keyMap = new MapWritable();
		keyMap.put(new Text("key"), key);
		System.out.println(keyMap.toString() + "\t" + valueList.toString());
		context.write(keyMap, valueList);
		*/
	}

    //Sets up the Cassandra connection to be used by the Reducer
    public void setup(Context context) {
        cclient.createConnection(""); 
    }

    //Closes the Cassandra connection after the Reducer is done
    public void cleanup(Context context) {
        cclient.closeConnection();
    }

}