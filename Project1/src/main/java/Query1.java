import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Query1 {
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            NullWritable out = NullWritable.get();
            //1,Dmauen Btcevjnlvhctq,24,Female,2,7628.92
            String line = value.toString();
            String[] personInfo = line.split(",");
            int age = Integer.parseInt(personInfo[2]);
            if (age>=20 && age<=50) {
                Text t1 = new Text(personInfo[1]);
                context.write(t1,out);
            }

        }
    }
    public static void main(String[] args) throws Exception {
        // args[0] is customer file path
        // args[1] is output path
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Query1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileSystem fs = FileSystem.getLocal(conf);
        Path p = new Path(args[1]);
        if(fs.exists(p)){
            fs.delete(p, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}