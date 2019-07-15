package cs455.hadoop.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class Job1 {
    public static void main(String[] args) {
        try {
//            Configuration conf = new Configuration();
//            Job job = Job.getInstance(conf, "Q1-6");
//            job.setJarByClass(Job1.class);
            // Mapper
//             job.setMapperClass(Mapper1.class);
            // Combiner. We use the reducer as the combiner in this case.
            // Dont set this unless I use combiner Piazza @160
            //job.setCombinerClass(Reducer1.class);
//            job.setReducerClass(Reducer1.class);
 //           job.setInputFormatClass(TextInputFormat.class);
//
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);

//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);

            //Setting up multiple filepaths for same job
            String metaPath = args[0] + "/metadata";
            String analysisPath = args[0] + "/analysis";

 //           MultipleInputs.addInputPath(job, new Path(metaPath), TextInputFormat.class, Mapper1.class);
 //          MultipleInputs.addInputPath(job, new Path(analysisPath), TextInputFormat.class, Mapper2.class);

//             path to output in HDFS
   //         FileOutputFormat.setOutputPath(job, new Path(args[1]));

//             Block until the job is completed.
 //           job.waitForCompletion(true);

            //Setup Q7-9 job
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Q7-9");
            job2.setJarByClass(Job1.class);
            
            job2.setReducerClass(Reducer2.class);
            job2.setInputFormatClass(TextInputFormat.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job2, new Path(metaPath), TextInputFormat.class, Mapper3.class);
            MultipleInputs.addInputPath(job2, new Path(analysisPath), TextInputFormat.class, Mapper4.class);

            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);

        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
