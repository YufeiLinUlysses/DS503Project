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

public class Query5 {
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
                        int age = Integer.parseInt(splitted[2]);
                        String tag = "";
                        if(age < 20){
                            tag = "[10,20)";
                        } else if(age >=20 && age <30){
                            tag = "[20,30)";
                        } else if(age >=30 && age <40){
                            tag = "[30,40)";
                        } else if(age >=40 && age <50){
                            tag = "[40,50)";
                        } else if(age >=50 && age <60){
                            tag = "[50,60)";
                        } else{
                            tag = "[60,70]";
                        }
                        String cid = splitted[0];
                        String gender = splitted[3];
                        customer.put(cid,tag + "    " +gender);
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
            String groupInfo = customer.get(cid);
            String str = String.format("%s",transInfo[2]);
            context.write(new Text(groupInfo),new Text(str));
        }
    }
    public static class MyCombiner extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            double min = 1001;
            double max = 0;
            double total = 0;
            int cnt = 0;
            for (Text t : values)
            {
                Double transTotal = Double.parseDouble(t.toString());
                if (min > transTotal) min = transTotal;
                if (max < transTotal) max = transTotal;
                cnt += 1;
                total += transTotal;
            }
            String str = String.format("%.2f,%.2f,%.2f,%d", min, max, total, cnt);
            context.write(new Text(key), new Text(str));
        }
    }

    public static class MyReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int cnt = 0;
            double min = 1001;
            double max = 0.0;
            double total = 0;
            for (Text t : values)
            {
                String[] val = t.toString().split(",");
                double val1 = Double.parseDouble(val[0]);
                if(min > val1) min = val1;
                double val2 = Double.parseDouble(val[1]);
                if(max < val2) max = val2;
                cnt += Integer.parseInt(val[3]);
                total += Double.parseDouble(val[2]);
            }
            String str = String.format("%.2f   %.2f   %.2f", total/cnt, min, max);
            context.write(key, new Text(str));
        }
    }
    public static void main(String[] args) throws Exception {
        // args[0] is customer file path
        // args[1] is transaction file path
        // args[2] is output path
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // add small table to cache
        job.addCacheFile(new URI(args[0]));

        job.setJarByClass(Query5.class);
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