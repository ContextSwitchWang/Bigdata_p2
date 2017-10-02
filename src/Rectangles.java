import java.util.Iterator;

public class Rectangles implements Iterable<String>{

  static int minX = 1, maxX = 10000, minY = 1, maxY = 10000;
  static int minSize = 1, maxSize = 100;
	@Override
	public Iterator<String> iterator() {
		return new Iter(1, 100000);
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
      int x = Util.randomInt(minX, maxX);
      int y = Util.randomInt(minX, maxX);
      int width = Util.randomInt(minSize, maxSize);
      int height = Util.randomInt(minSize, maxSize);
      int x2 = x + width;
      int y2 = y + height;
      if(x2 > maxX)
        x2 = maxX;
      if(y2 > maxY)
        y2 = maxY;
			return String.join(",",
				String.format("%d", id++),
				String.format("%d", x),
				String.format("%d", y),
				String.format("%d", x2),
				String.format("%d", y2));
		}
	}
}
