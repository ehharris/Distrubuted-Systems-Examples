package cs455.hadoop.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    //Hashmap so data can all be written in cleanup to avoid context switches
    HashMap<Text, Text> needWrite = new HashMap<>();

    /**
     * Mapper class for Analysis Files
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //regex to get rid of commas modified so it won't get rid of commas inside quotes
        String reg1 = " [^\"] ";
        String reg2 = String.format(" \" %s* \" ",  reg1);
        String regex = String.format("(?x) , (?= (?: %s* %s )* %s* $)", reg1, reg2, reg1);

        String fullData = value.toString();
        //splits Data by each line
        String[] splitData = fullData.split("\n");
        for(String data : splitData) {
            String[] delimData = data.split(regex);
            //Q1. In Mapper1
            //Q2. Which artist’s songs are the loudest on average?
            Q2Map(delimData);
            //Q3. What is the song with the highest hotttnesss (popularity) score?
            Q3Map(delimData);
            //Q4. Which artist has the highest total time spent fading in their songs?
            Q4and5Map(delimData);
            //Q5. Dataset needed already retrived from Q2 & Q4
            //Q6. What are the 10 most energetic and danceable songs? List them in descending order.
            Q6Map(delimData);
        }

    }

    //produces song_id,loudness
    private void Q2Map(String[] delimData){
            String song_id = delim1(delimData[1]);
            String strLoud = delim1(delimData[10]);
            if (!song_id.isEmpty() && !strLoud.isEmpty() &&
                    !song_id.equals("song_id") && !strLoud.equals("loudness")) {
                //'2a' is added for Q2 and analysis file
                song_id = "2a" + song_id;
                needWrite.put(new Text(song_id), new Text(strLoud));
            }
    }

    //produces song_id,hotttnesss
    private void Q3Map(String[] delimData){
            String song_id = delim1(delimData[1]);
            String strHot = delimData[2];
            if (!song_id.isEmpty() && !strHot.isEmpty() &&
                    !song_id.equals("song_id") && !strHot.equals("song_hotttnesss")){
                double hotness = Double.parseDouble(strHot);
                //added so it doesn't add a song if not rated/0 to cut down on time
                if (hotness > 0) {
                    //'3a' is added for Q3 and analysis file
                    song_id = "3a" + song_id;
                    needWrite.put(new Text(song_id), new Text(strHot));
                }
            }
    }

    //produces song_id, duration fading in sec/duration
    private void Q4and5Map(String[] delimData){
            String song_id = delim1(delimData[1]);
            String durationStr = delimData[5];
            String fadeInEndStr = delimData[6];
            String fadeOutBeginStr = delimData[13];
            //Takes out weird data
            if (!song_id.isEmpty() && !durationStr.isEmpty() &&
                    !song_id.equals("song_id") && !durationStr.equals("duration")) {
                double totalFade = 0;
                double durationTime = Double.parseDouble(durationStr);
                if(!fadeInEndStr.isEmpty()){
                    totalFade += Double.parseDouble(fadeInEndStr);
                }
                if(!fadeOutBeginStr.isEmpty()){
                    totalFade += (Double.parseDouble(durationStr) - Double.parseDouble(fadeOutBeginStr));
                }
                //'4(d/f)' is added for Q4
                //Takes out songs with 0 fade time/duration
                if(totalFade != 0 || durationTime == 0) {
                    String fsong_id = "4f" + song_id;
                    needWrite.put(new Text(fsong_id), new Text(Double.toString(totalFade)));
                }
                String dsong_id = "4d" + song_id;
                needWrite.put(new Text(dsong_id), new Text(Double.toString(durationTime)));

            }
    }

    //produces danceability, energy
    private void Q6Map(String[] delimData){
            String song_id = delimData[1];
            String danceability = delimData[4];
            String energy = delimData[7];
            if (!song_id.isEmpty() && !energy.isEmpty() &&
                    !song_id.equals("song_id") && !energy.equals("energy")){
                song_id = "6e" + song_id;
                needWrite.put(new Text(song_id), new Text(energy));
            }
            if (!song_id.isEmpty() && !danceability.isEmpty() &&
                    !song_id.equals("song_id") && !danceability.equals("danceability")){
                song_id = "6d" + song_id;
                needWrite.put(new Text(song_id), new Text(danceability));
            }
    }

    //gets rid of '\' and sometimes 'b'
    private String delim1(String data) {
        data = data.replace("\"", "");
        data = data.replace("'", "");
        //some data have a 'b' before the name, idk why
        if (data.charAt(0) == 'b') {
            data = data.substring(1);
        }
        return data;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator it = needWrite.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Text, Text> val = (Map.Entry) it.next();
            context.write(val.getKey(), val.getValue());
        }
    }
}
