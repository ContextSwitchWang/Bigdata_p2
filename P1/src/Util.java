import java.util.Random;
public class Util {
	static Random ran = new Random();
	static String randomString(int min, int max) {
		int len = ran.nextInt(max-min)+min;
		StringBuilder sb = new StringBuilder();
		sb.append('"');
		for(int i : ran.ints(len, 65, 122).toArray()) {
			sb.append((char)i);
		}
		sb.append('"');
		return sb.toString();
	}
	
	static int randomInt(int min, int max) {
		return ran.nextInt(max-min)+min;
	}
}
