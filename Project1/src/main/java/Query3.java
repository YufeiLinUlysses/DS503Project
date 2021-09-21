import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Query3 {
    public static class CustMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        //1,Dmauen Btcevjnlvhctq,24,Female,2,7628.92

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] personInfo = line.split(",");
            context.write(new Text(personInfo[0]),new Text("C," + personInfo[1] +","+ personInfo[5]));
        }
    }
    public static class TransMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        //1,42181,748.14,5,Z5XzG`0NLrNE*VM8mWTqzI]}yT\^r$c[c~5[iWOS sX?

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] transInfo = line.split(",");
            context.write(new Text(transInfo[1]),new Text("T," + transInfo[2]+ "," + transInfo[3]));
        }
    }
    public static class MyCombiner extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            String pInfo = "";
            double total = 0.0;
            int cnt = 0;
            int minNumber = 11;
            for (Text t : values)
            {
                String[] val = t.toString().split(",");
                if (val[0].equals("T"))
                {
                    cnt++;
                    total += Double.parseDouble(val[1]);
                    minNumber = Math.min(minNumber, Integer.parseInt(val[2]));

                }
                else if (val[0].equals("C"))
                {
                    pInfo = val[1] + "-" + val[2];
                }
            }
            String str = String.format("%s,%d,%.2f,%d", pInfo, cnt, total,minNumber);
            context.write(key, new Text(str));
        }
    }

    public static class MyReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            String pInfo = "";
            double total = 0.0;
            int cnt = 0;
            int minNumber = 11;
            for (Text t : values)
            {
                String[] val = t.toString().split(",");
                if(pInfo == "") pInfo = val[0];
                cnt += Integer.parseInt(val[1]);
                total += Double.parseDouble(val[2]);
                minNumber = Math.min(minNumber, Integer.parseInt(val[3]));
            }
            String[] needs = pInfo.split("-");
            String str = String.format("%s   %s   %d   %.2f   %d", needs[0],needs[1], cnt, total, minNumber);
            context.write(key, new Text(str));
        }
    }
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Query2.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Multiple input
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TransMapper.class);

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