import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TMatrix {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, LongWritable, MapWritable>{

        private LongWritable col = new LongWritable(0);
        private MapWritable cell = new MapWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            // Split line into cells
            String[] cells = value.toString().split(",");

            for(int i=0; i<cells.length; i++){

                // The key is the column index, which will become the line index
                col.set(i);

                // Current line which will become the new column index
                cell.put(new Text("line"), new LongWritable(key.get()));
                // Value of the cell
                cell.put(new Text("value"), new Text(cells[i]));

                // (column, (line, value))
                context.write(col, cell);
            }
        }
    }

    public static class IntTransposeReducer
            extends Reducer<LongWritable, MapWritable, LongWritable, Text> {

        private Text result = new Text();

        public void reduce(LongWritable key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Generate a new line: CSV line
            Text line = new Text();

            // Construct the array with all cells of a line (initialisation)
            ArrayList<MapWritable> cells = new ArrayList<>();

            // Fill the array cells
            for (MapWritable val : values) {
                MapWritable cell = new MapWritable();

                // Receive the new position: new column = ex line
                LongWritable newCol = (LongWritable) val.get(new Text("line"));

                // Receive the value
                Text cellValue = (Text) val.get(new Text("value"));

                cell.put(new Text("col"), newCol);
                cell.put(new Text("value"), cellValue);
                cells.add(cell);
            }


            // Need to sort cells in the line
            cells.sort(Comparator.comparingLong(a -> ((LongWritable) a.get(new Text("col"))).get()));

            /*
            Now, we can run the line because cells are sorted
            Write in the new line, cell by cell
            */
            int s = cells.size()-1;
            int i =0;
            for (MapWritable cell : cells){
                Text cellText = (Text) cell.get(new Text("value"));
                // Add cell value to the new line
                line.append(cellText.getBytes(),0,cellText.getLength());
                // Put a "," between values,
                // To not write "," after the last cell
                if (s > i) {
                    line.append(new Text(",").getBytes(), 0, 1);
                }
                i++;
            }


            result.set(line.toString());

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Transposed matrix");
        job.setJarByClass(TMatrix.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntTransposeReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // configuration contains reference to the named node
        FileSystem fs = FileSystem.get(conf);

        // remove output folder if he exists
        // otherwise the map/reduce couldn't perform
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}