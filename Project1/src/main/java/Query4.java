import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.io.BufferedReader;

import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Query4 {
    public static class TransMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, String> customer = new HashMap<>();
        // Read in Customer dataset and store into a hashmap
        public void setup(Context context) throws IOException, InterruptedException{
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0)
            {
                try{
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path path = new Path(cacheFiles[0].toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line = null;
                    while((line = reader.readLine()) != null) {
                        String[] splitted = line.split(",");
                        String cid = splitted[0];
                        String cc = splitted[4];
                        customer.put(cid,cc);
                    }
                } catch(IOException ex) {
                    System.err.println("Exception in mapper setup: " + ex.getMessage());
                }
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] transInfo = line.split(",");
            String cid = transInfo[1];
            String countryCode = customer.get(cid);
            String str = String.format("%s",transInfo[2]);
            context.write(new Text(countryCode + "," + cid),new Text(str));
        }
    }
    public static class MyCombiner extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            double min = 1001;
            double max = 0;
            for (Text t : values)
            {
                Double transTotal = Double.parseDouble(t.toString());
                if (min > transTotal) min = transTotal;
                if (max < transTotal) max = transTotal;
            }
            String[] keys = key.toString().split(",");
            String str = String.format("%s,%.2f,%.2f", keys[1], min, max);
            context.write(new Text(keys[0]), new Text(str));
        }
    }

    public static class MyReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            HashSet<String> customer = new HashSet<>();
            double min = 1001;
            double max = 0.0;
            for (Text t : values)
            {
                String[] val = t.toString().split(",");
                double val1 = Double.parseDouble(val[1]);
                if(min > val1) min = val1;
                double val2 = Double.parseDouble(val[2]);
                if(max < val2) max = val2;
                customer.add(val[0]);
            }
            String str = String.format("%d   %.2f   %.2f", customer.size(), min, max);
            context.write(key, new Text(str));
        }
    }
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // add small table to cache
        job.addCacheFile(new URI(args[0]));

        job.setJarByClass(Query4.class);
        job.setMapperClass(TransMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        //Output setup
        FileSystem fs = FileSystem.getLocal(conf);
        Path p = new Path(args[2]);
        if(fs.exists(p)){
            fs.delete(p, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean status = job.waitForCompletion(true);
        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}