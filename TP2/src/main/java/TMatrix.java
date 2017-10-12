import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TMatrix {

    public static class TokenizerMapper
            extends Mapper<IntWritable, Text, IntWritable, MapWritable>{

        private IntWritable col = new IntWritable(0);
        private MapWritable cell = new MapWritable();

        public void map(IntWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            // Split line into cells
            String[] cells = value.toString().split(",");

            StringTokenizer itr = new StringTokenizer(cells.toString());
            while (itr.hasMoreTokens()) {

                IntWritable line = new IntWritable(key.get());

                // Current line which will become the new column index
                cell.put(new Text("line"), line);
                // Value of the cell
                cell.put(new Text("value"), new Text(itr.nextToken()));

                // Write current column which will become the new line index
                // (column, (line, value))
                context.write(col, cell);

                // Increment column index
                col.set(col.get()+1);
            }
        }
    }

    public static class IntTransposeReducer
            extends Reducer<IntWritable, MapWritable, IntWritable, Text> {

        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Generate a new line: CSV line
            StringBuilder line = new StringBuilder();

            // Construct the array with all cells of a line (initialisation)
            ArrayList<MapWritable> cells = new ArrayList<MapWritable>();

            // Fill the array cells
            for (MapWritable val : values) {
                MapWritable cell = new MapWritable();

                // Receive the new position: new column = ex line
                IntWritable newCol = (IntWritable) val.get(new Text("line"));

                // Receive the value
                Text cellValue = (Text) val.get(new Text("value"));

                cell.put(new Text("col"), newCol);
                cell.put(new Text("value"), cellValue);
                cells.add(cell);
            }


            int indexCol = 0;
            // Write in the new line
            for (MapWritable cell : cells){
                int newCol = ((IntWritable) cell.get(new Text("position"))).get();
                // Write in respect of the initial position
                if (newCol == indexCol) {
                    Text cellText = (Text) cell.get(new Text("value"));
                    // Add cell value to the new line
                    line.append(cellText).append(",");
                }
                indexCol++;
            }

            // Remove last character because it's a ","
            if(line.length() > 0){
                line.setLength(line.length() - 1);
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

        //job.setOutputKeyClass(IntWritable.class);
        //job.setOutputValueClass(MapWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
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