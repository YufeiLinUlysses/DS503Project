import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Query4 {
    public class MapClass extends Mapper{

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set stopWords = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                    for(Path stopWordFile : stopWordsFiles) {
                        readFile(stopWordFile);
                    }
                }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }
        }

        /**
         * map function of Mapper parent class takes a line of text at a time
         * splits to tokens and passes to the context as word along with value as one
         */
        public void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line," ");

            while(st.hasMoreTokens()){
                String wordText = st.nextToken();

                if(!stopWords.contains(wordText.toLowerCase())) {
                    word.set(wordText);
                    context.write(word,one);
                }
            }

        }

        private void readFile(Path filePath) {
            try{
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
                String stopWord = null;
                while((stopWord = bufferedReader.readLine()) != null) {
                    stopWords.add(stopWord.toLowerCase());
                }
            } catch(IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
            }
        }
    }

    public class ReduceClass extends Reducer{

        /**
         * Method which performs the reduce operation and sums
         * all the occurrences of the word before passing it to be stored in output
         */
        public void reduce(Text key, Iterable values,
                              Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            Iterator valuesIt = values.iterator();

            while(valuesIt.hasNext()){
                sum = sum + valuesIt.next().get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

//        job.setJobName("Word Counter With Stop Words Removal");

        //Add input and output file paths to job based on the arguments passed
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));



        //Set the MapClass and ReduceClass in the job



        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Query4.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        // load small table
        DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());

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