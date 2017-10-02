import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class DataGen {
	public static void write(String path, Iterable<String> what)
    throws FileNotFoundException {
		PrintWriter writer = new PrintWriter(path);
		for(String line : what) {
			writer.print(line);
			writer.print("\n");
		}
		writer.close();
	}
	public static void main(String [] args) throws FileNotFoundException {
		write("Points.txt", new Points());
		write("Rectangles.txt", new Rectangles());
	}
}
