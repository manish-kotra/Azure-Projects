import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class KMeans_k5 {

	public static class Point {
		double x, y;
		
		public Point(String s) {
			String[] tokens = s.split(",");
			x = Double.parseDouble(tokens[0]);
			y = Double.parseDouble(tokens[1]);
		}

		public Point(double x, double y){
			this.x = x;
			this.y = y;
		}

		public String toString(){
			return x + "," + y;
		}

		public double distance(Point p){
			return Math.sqrt(Math.pow((x - p.x),2) + Math.pow((y - p.y),2));
		}

		public static Point average(List<Point> points){
			double sumX = 0; 
			double sumY = 0;
			for (Point p : points){
				sumX += p.x;
				sumY += p.y;
			}
			return new Point(sumX/points.size(), sumY/points.size());
		}
	}

	public static class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {

		// centroids : Linked-list/arraylike
		private List<Point> centers = new ArrayList<>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			Configuration conf = context.getConfiguration();

			// retrive file path
			Path centroids = new Path(conf.get("centroid.path"));

			// create a filesystem object
			FileSystem fs = FileSystem.get(conf);

			// create a file reader
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centroids));

			// read centroids from the file and store them in a centroids variable
			Text key = new Text();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				centers.add(new Point(key.toString()));
			}
			reader.close();

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input -> key: charater offset, value -> a point (in Text)
			// write logic to assign a point to a centroid
			// emit key (centroid id/centroid) and value (point)
			Point point = new Point(value.toString());
			Point nearestCenter = null;
			double minDistance = Double.MAX_VALUE;
			for (Point center : centers){
				double distance = point.distance(center);
				if (distance < minDistance){
					minDistance = distance;
					nearestCenter = center;
				}
			}
			context.write(new Text(nearestCenter.toString()), value);
		}

		// @Override
		// public void cleanup(Context context) throws IOException, InterruptedException {

		// }

	}

	public static class PointsReducer extends Reducer<Text, Text, Text, Text> {

		private List<Point> newCentroids = new ArrayList<>();

		// public static enum Counter {
		// 	CONVERGED
		// }
		// new_centroids (variable to store the new centroids

		// @Override
		// public void setup(Context context) {

		// }

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Input: key -> centroid id/centroid , value -> list of points
			// calculate the new centroid
			// new_centroids.add() (store updated cetroid in a variable)
			List<Point> points = new ArrayList<>();
			for (Text value : values){
				points.add(new Point(value.toString()));
			}
			Point newCentroid = Point.average(points);
			newCentroids.add(newCentroid);
			context.write(new Text(newCentroid.toString()), new Text(""));

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			// BufferedWriter
			// delete the old centroids
			// write the new centroids
			Configuration conf = context.getConfiguration();
            Path centroidPath = new Path(conf.get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(centroidPath)) {
                fs.delete(centroidPath, true);
            }
			// SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, SequenceFile.Writer.file(centroidPath),
            //         SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
	    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(centroidPath),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(IntWritable.class));

            
            // SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, centroidPath, Text.class, IntWritable.class);
            IntWritable value = new IntWritable(0);
            for (Point centroid : newCentroids) {
                writer.append(new Text(centroid.toString()), value);
            }
            writer.close();
		}

	}

	public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: KMeans <in> <out>");
            System.exit(2);
        }

		Path center_path = new Path("centroid/cen.seq");
		conf.set("centroid.path", center_path.toString());

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(center_path)) {
			fs.delete(center_path, true);
		}

		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(center_path),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(IntWritable.class));

        final IntWritable value = new IntWritable(0);
		// centerWriter.append(new Text("50.197031637442876,32.94048164287042"), value);
		// centerWriter.append(new Text("43.407412339767056,6.541037020010927"), value);
		// centerWriter.append(new Text("1.7885358732482017,19.666057053079573"), value);
		// centerWriter.append(new Text("32.6358540480337,4.03843047564191"), value);
		// centerWriter.append(new Text("32.6358540480337,4.03843047564191"), value);

		// centerWriter.close();

		Random random = new Random();

        int min = -10;
        int max = 50;
	int k = 5;
        for (int i = 0; i < k; i++) {
            double x = min + (max - min) * random.nextDouble();
            double y = min + (max - min) * random.nextDouble();
            String centroid = x + "," + y;
            centerWriter.append(new Text(centroid), value);
        }
        centerWriter.close();

	for (int itr = 0; itr < 15; itr++){
			// config
			// job
			// set the job parameters
	    Job job = Job.getInstance(conf, "KMeans Iteration " + itr);
            job.setJarByClass(KMeans_k5.class);
            job.setMapperClass(PointsMapper.class);
            job.setReducerClass(PointsReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + itr));
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
		}

		// read the centroid file from hdfs and print the centroids (final result)
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(center_path));
        Text key = new Text();
        IntWritable centroidValue = new IntWritable();
        while (reader.next(key, centroidValue)) {
            System.out.println(key.toString());
        }
        reader.close();
	}

}
