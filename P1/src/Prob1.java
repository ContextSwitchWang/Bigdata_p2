import java.util.HashSet;
import java.util.LinkedList;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Prob1 extends Configured implements Tool {
  private Prob1() {}                               // singleton
  static int maxId = 100000;
  public static boolean within(int x, int y, int[] b) {
    if(x < b[0]) {
      return false;
    }
    if(x > b[2]) {
      return false;
    }
    if(y > b[3]) {
      return false;
    }
    if(y < b[1]) {
      return false;
    }
    return true;
  }
  public static boolean within(int[] a, int[] b) {
    if(a[2] < b[0]) {
      return false;
    }
    if(a[0] > b[2]) {
      return false;
    }
    if(a[1] > b[3]) {
      return false;
    }
    if(a[3] < b[1]) {
      return false;
    }
    return true;
  }
  public static class MyWritable implements Writable {
    public int x, y;
    public MyWritable(int x, int y) {
      this.x = x;
      this.y = y;
    }
    public void write(DataOutput out) throws IOException {
      out.writeInt(x);
      out.writeChar(',');
      out.writeInt(y);
    }
    public void readFields(DataInput in) throws IOException {
      x = in.readInt();
      in.readChar();
      y = in.readInt();
    }
    @Override
    public String toString() {
      return String.format("%d,%d", x, y);
    }
  }
  public static class MyMapper extends Mapper<
          LongWritable, Text, IntWritable, MyWritable> {
    int[] W;
    static int binSize = 100;
    ArrayList<LinkedList<int[]>> table;
    int tableSize;
    public MyMapper() {}
    public int hash(int x, int y) {
      int L = (W[2] - W[0])/binSize;
      int key = (x - W[0])/binSize + L*((y - W[1])/binSize);
      return key;
    }
    void addToTable(int[] rect) {
      HashSet<Integer> set = new HashSet<Integer>(4);
      set.add(hash(rect[0], rect[1]));
      set.add(hash(rect[0], rect[3]));
      set.add(hash(rect[2], rect[1]));
      set.add(hash(rect[2], rect[3]));
      for(int v : set) {
        if(v < 0 || v >= tableSize) {
          continue;
        }
        table.get(v).add(rect);
      }
    }
    @Override
    protected void setup(MyMapper.Context context)
                  throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      W = conf.getInts("W");
      tableSize = ((W[2]-W[0])/binSize+1)*((W[3]-W[1])/binSize+1);
      System.out.print("Creating table of size ");
      System.out.println(tableSize);
      table = new ArrayList<LinkedList<int[]>>(tableSize);
      for(int i = 0; i < tableSize; i++) {
        table.add(new LinkedList<int[]>());
      }
      BufferedReader in
        = new BufferedReader(new FileReader(conf.get("Rectangles")));
      String line;
      while ((line = in.readLine()) != null) {
        String[] arr = line.split(",");
        int[] rect = new int[5];
        for(int i = 0; i < 4; i++) {
          rect[i+1] = Integer.parseInt(arr[i]);
        }
        rect[4] = Integer.parseInt(arr[0]);
        if(!within(rect, W)) {
          continue;
        }
        addToTable(rect);
      }
    }
    @Override
    public void map(LongWritable key, Text value, MyMapper.Context context) throws IOException, InterruptedException {
        String a = value.toString();
        String[] arr = a.split(",");
        int x = Integer.parseInt(arr[0]);
        int y = Integer.parseInt(arr[1]);
        if(!within(x, y, W)) {
          return;
        }
        int k = hash(x, y);
        if(k < 0 || k >= tableSize) {
          System.out.println(k);
          System.out.println("Impossible Error!");
        }
        for(int[] rect: table.get(hash(x, y))) {
          if(within(x, y, rect)) {
            context.write(new IntWritable(rect[4]), new MyWritable(x, y));
          }
        }
    }
  }
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Prob1 Output Points Rectangles W");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }
    Configuration conf = getConf();
    conf.set("Rectangles", args[2]);
    if (args.length == 4) {
      conf.set("W", args[3]);
    }
    else {
      conf.set("W", "1,1,10000,10000");
    }
    Job job = Job.getInstance(conf);

    job.setJarByClass(Prob1.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MyMapper.class);


    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MyWritable.class);
    job.waitForCompletion(true);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Prob1(), args);
    System.exit(res);
  }
}

