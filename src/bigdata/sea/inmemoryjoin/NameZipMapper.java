package bigdata.sea.inmemoryjoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

		FileSystem fileSystem = FileSystem.get(conf);
		Path filePath = new Path(fileName);
		FileStatus[] fileSystemStatus = fileSystem.listStatus(filePath);
		for (FileStatus status : fileSystemStatus) {
			Path userDataPath = status.getPath();
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fileSystem.open(userDataPath)));

			String profile = null;
			do {
				profile = br.readLine();
				if (profile != null) {
					String[] userData = profile.split(",");
					if (userData.length > 1) {
						usersMap.put(userData[0].trim(), profile);
					} else {
						usersMap.put(userData[0].trim(), "");
					}
				}
			} while (profile != null);
		}
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
					if (usersMap != null && usersMap.containsKey(friend)) {
						String[] friendData = usersMap.get(friend).split(",");
						if (friendData.length >= 6)
							context.write(new Text(friend), new Text(
									friendData[1] + ":" + friendData[6]));
					}
				}
			}
		}

	}
}
