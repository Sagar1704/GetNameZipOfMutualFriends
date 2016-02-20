package bigdata.sea.inmemoryjoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NameZipReducer extends Reducer<Text, Text, Text, Text> {
	private StringBuilder mutualFriends;

	public NameZipReducer() {
		mutualFriends = new StringBuilder();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		for (Text text : values) {
			mutualFriends.append(text.toString() + ",");
		}

	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String[] friends = mutualFriends.toString().split(",");
		if (friends.length >= 2)
			context.write(
					new Text(conf.get(NameZipApplication.USER_A) + ", "
							+ conf.get(NameZipApplication.USER_B)),
					new Text(mutualFriends.substring(0,
							mutualFriends.length() - 1)));
	}
}
