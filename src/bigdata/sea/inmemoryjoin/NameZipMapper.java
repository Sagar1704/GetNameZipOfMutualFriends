package bigdata.sea.inmemoryjoin;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NameZipMapper extends Mapper<Object, Text, Text, Text> {
	private String userA;
	private String userB;
	private String fileName;
	private Map<String, String> usersMap;

	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		usersMap = new HashMap<String, String>();
		Configuration conf = context.getConfiguration();
		fileName = conf.get(NameZipApplication.FRIENDS);
		Scanner scanner = new Scanner(new File(fileName));
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] userData = line.split("\t");
			if (userData.length == 2)
				usersMap.put(userData[0], userData[1]);
			else
				usersMap.put(userData[0], null);
		}
		scanner.close();
	}

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		userA = conf.get(NameZipApplication.USER_A);
		userB = conf.get(NameZipApplication.USER_B);

		String profile[] = value.toString().split("\t");
		String user = profile[0];

		if (profile.length == 2) {
			if (user.equalsIgnoreCase(userA) || user.equalsIgnoreCase(userB)) {
				for (String friend : profile[1].split(",")) {
					String[] friendData = usersMap.get(friend).split(",");
					context.write(new Text(friend),
							new Text(friendData[1] + ":" + friendData[6]));
				}
			}
		}

	}
}
