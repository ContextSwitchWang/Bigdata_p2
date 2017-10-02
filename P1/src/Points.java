import java.util.Iterator;

public class Points implements Iterable<String>{

  static int minX = 1, maxX = 10000, minY = 1, maxY = 10000;
	@Override
	public Iterator<String> iterator() {
		return new Iter(1, 11000000);
	}
	public class Iter implements Iterator<String> {
		int id;
		int maxId;
		public Iter(int id, int maxId) {
			this.id = id;
			this.maxId = maxId;
		}
		@Override
		public boolean hasNext() {
			return id <= maxId;
		}

		@Override
		public String next() {
      id ++;
			return String.join(",",
				String.format("%d", Util.randomInt(minX, maxX)),
				String.format("%d", Util.randomInt(minY, maxY)));
		}
	}
}
