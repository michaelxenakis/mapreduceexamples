import org.apache.hadoop.io.ArrayWritable;

public class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable(String[] strings) {
		super(strings);
	}
	
	@Override
	public String toString() {
		String strings = "";
		for (int idx = 0; idx < this.get().length; idx++) {
			strings = strings + " " + this.get()[idx].toString();
		}
		return strings;
	}
	
	public String printStrings() {
		String strings = "";
		for (int idx = 0; idx < this.get().length; idx++) {
			strings = strings + " " + this.get()[idx].toString();
		}
		return strings;
	}
}
