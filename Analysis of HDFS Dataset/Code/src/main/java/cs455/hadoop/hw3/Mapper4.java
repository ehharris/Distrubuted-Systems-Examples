package cs455.hadoop.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {

    //Hashmap so data can all be written in cleanup to avoid context switches
    HashMap<Text, Text> needWrite = new HashMap<>();

    //For Q7 to lighten the data load
    HashMap<Text, Double>avgStartTimeHash= new HashMap<>();
    HashMap<Text, Double>avgPitchTimeHash= new HashMap<>();
    HashMap<Text, Double>avgTimbresHash= new HashMap<>();
    HashMap<Text, Double>avgLoudMaxHash= new HashMap<>();
    HashMap<Text, Double>avgLoudMaxTimeHash= new HashMap<>();
    HashMap<Text, Double>avgStartLoudHash = new HashMap<>();
    HashMap<Text, Integer>avgNumSegsHash = new HashMap<>();


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
            Q2Map(delimData);
            //Q7. Create segment data for the average song. Include start time, pitch, timbre, max loudness,
            //max loudness time, and start loudness.
            Q7Map(delimData);
            //Q8. In Mapper1
            //Q9. Imagine a song with a higher hotttnesss score than the song in your answer to Q3. List this
            //song’s tempo, time signature, danceability, duration, mode, energy, key, loudness, when it
            //stops fading in, when it starts fading out, and which terms describe the artist who made it.
            //Give both the song and the artist who made it unique names.
            Q9Map(delimData);
            Q9MapHot(delimData);
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

    //produces song_id, start time/pitch/timbre/max loudness/max loudness time/start loudness/numSegments
    private void Q7Map(String[] delimData){
        if(!delimData[18].isEmpty() && !delimData[18].equals("segments_start") &&
                !delimData[20].isEmpty() && !delimData[18].equals("segments_pitches") &&
                !delimData[21].isEmpty() && !delimData[21].equals("segments_timbre") &&
                !delimData[22].isEmpty() && !delimData[22].equals("segments_loudness_max") &&
                !delimData[23].isEmpty() && !delimData[23].equals("segments_loudness_max_time") &&
                !delimData[24].isEmpty() && !delimData[24].equals("segments_loudness_start") &&
                !delimData[1].isEmpty() && !delim1(delimData[1]).equals("song_id ")) {

            //start time, pitch, timbre, max loudness, max loudness time, start loudness, song_id, numSegments
            String[] startTimes = delimData[18].split(" ");
            String[] pitchTimes = delimData[20].split(" ");
            String[] timbres = delimData[21].split(" ");
            String[] loudMax = delimData[22].split(" ");
            String[] loudMaxTimes = delimData[23].split(" ");
            String[] startLoud = delimData[24].split(" ");
            String song_id = delim1(delimData[1]);

            //calculate the average of each song
            Double avgStartTime = 0.0, avgPitchTime = 0.0, avgTimbres = 0.0,
                    avgLoudMax = 0.0, avgLoudMaxTime = 0.0, avgStartLoud = 0.0;
            int count;
            for (count = 0; count < startTimes.length - 1; count++) {
                avgStartTime += Double.parseDouble(startTimes[count]);
            }
            int count1;
            for (count1 = 0; count1 < pitchTimes.length - 1; count1++) {
                avgPitchTime += Double.parseDouble(pitchTimes[count1]);
            }
            int count2;
            for ( count2 = 0; count2 < timbres.length - 1; count2++) {
                avgTimbres += Double.parseDouble(timbres[count2]);
            }
            int count3;
            for ( count3 = 0; count3 < loudMax.length - 1; count3++) {
                avgLoudMax += Double.parseDouble(loudMax[count3]);
            }
            int count4;
            for ( count4 = 0; count4 < loudMaxTimes.length - 1; count4++) {
                avgLoudMaxTime += Double.parseDouble(loudMaxTimes[count4]);
            }
            int count5;
            for ( count5 = 0; count5 < startLoud.length - 1; count5++) {
                avgStartLoud += Double.parseDouble(startLoud[count5]);
            }

            avgStartTime = avgStartTime / count;
            avgPitchTime = avgPitchTime / count1;
            avgTimbres = avgTimbres / count2;
            avgLoudMax = avgLoudMax / count3;
            avgLoudMaxTime = avgLoudMaxTime / count4;
            avgStartLoud = avgStartLoud / count5;

            //send averages to mapper
            avgStartTimeHash.put(new Text("7s" + song_id), avgStartTime);
            avgPitchTimeHash.put(new Text("7p" + song_id), avgPitchTime);
            avgTimbresHash.put(new Text("7t" + song_id), avgTimbres);
            avgLoudMaxHash.put(new Text("7lm" + song_id), avgLoudMax);
            avgLoudMaxTimeHash.put(new Text("7lmt" + song_id), avgLoudMaxTime);
            avgStartLoudHash.put(new Text("7ls" + song_id), avgStartLoud);
            avgNumSegsHash.put(new Text("7a" + song_id), count1);
        }
    }

    //produces song_id,TimeSig/Mode/Key/tempo of only hot songs
    private void Q9Map(String[] delimData){
        Double hotVal = 0.1;
        if (!delimData[1].isEmpty() && !delimData[1].equals("song_id") && !delimData[2].isEmpty() ){
        if(Double.parseDouble(delimData[2]) >= hotVal){
                hotVal = Double.parseDouble(delimData[2]);
            String song_id = delim1(delimData[1]);
            if (!delimData[15].isEmpty() && !delimData[15].equals("time_signature")) {
                String timeSig = delimData[15];
                needWrite.put(new Text("9s" + song_id), new Text(timeSig));
            }
            if (!delimData[11].isEmpty() && !delimData[11].equals("mode")) {
                String mode = delimData[11];
                needWrite.put(new Text("9m" + song_id), new Text(mode));
            }
            if (!delimData[8].isEmpty() && !delimData[8].equals("key")) {
                String key = delimData[8];
                needWrite.put(new Text("9k" + song_id), new Text(key));
            }
            if (!delimData[14].isEmpty() && !delimData[14].equals("tempo")) {
                String key = delimData[14];
                needWrite.put(new Text("9t" + song_id), new Text(key));
            }
            if (!delimData[5].isEmpty() && !delimData[5].equals("duration")) {
                String key = delimData[5];
                needWrite.put(new Text("9d" + song_id), new Text(key));
            }
            if (!delimData[10].isEmpty() && !delimData[10].equals("loudness")) {
                String key = delimData[10];
                needWrite.put(new Text("9l" + song_id), new Text(key));
            }
            if (!delimData[6].isEmpty() && !delimData[6].equals("end_of_fade_in")) {
                String key = delimData[6];
                needWrite.put(new Text("9fi" + song_id), new Text(key));
            }
            if (!delimData[13].isEmpty() && !delimData[13].equals("start_of_fade_out")) {
                String key = delimData[13];
                needWrite.put(new Text("9fo" + song_id), new Text(key));
            }
            }
        }
    }

    //produces song_id,artName only if song is hot
    private void Q9MapHot(String[] delimData){
        String song_id = delim1(delimData[1]);
        String strHot = delimData[2];
        String artName = delim1(delimData[7]);
        if (!song_id.isEmpty() && !strHot.isEmpty() && !artName.isEmpty() &&
                !song_id.equals("song_id") && !strHot.equals("song_hotttnesss") && !artName.equals("artist_name")){
            double hotness = Double.parseDouble(strHot);
            //added so it doesn't add a song if not rated/0 to cut down on time
            if (hotness == 1) {
                //'3a' is added for Q3 and analysis file
                song_id = "9h" + song_id;
                needWrite.put(new Text(song_id), new Text(artName));
            }
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
        cleanupHashMaps();
        Iterator it = needWrite.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Text, Text> val = (Map.Entry) it.next();
            context.write(val.getKey(), val.getValue());
        }
    }
    //calculates avgs before reducer to avoid running out of mem
    private void cleanupHashMaps(){
        Double avg = 0.0;
        for(Map.Entry<Text, Double> entry: avgStartTimeHash.entrySet()) {
            avg += entry.getValue();
        }
        avg = avg/avgStartTimeHash.size();
        needWrite.put(new Text("7s"), new Text(avg.toString()));

        avg = 0.0;
        for(Map.Entry<Text, Double> entry: avgPitchTimeHash.entrySet()) {
            avg += entry.getValue();
        }
        avg = avg/avgPitchTimeHash.size();
        needWrite.put(new Text("7p"), new Text(avg.toString()));

        avg = 0.0;
        for(Map.Entry<Text, Double> entry: avgTimbresHash.entrySet()) {
            avg += entry.getValue();
        }
        avg = avg/avgTimbresHash.size();
        needWrite.put(new Text("7t"), new Text(avg.toString()));

        avg = 0.0;
        for(Map.Entry<Text, Double> entry: avgLoudMaxHash.entrySet()) {
            avg += entry.getValue();
        }
        avg = avg/avgLoudMaxHash.size();
        needWrite.put(new Text("7lm"), new Text(avg.toString()));

        avg = 0.0;
        for(Map.Entry<Text, Double> entry: avgLoudMaxTimeHash.entrySet()) {
            avg += entry.getValue();
        }
        avg = avg/avgLoudMaxTimeHash.size();
        needWrite.put(new Text("7lmt"), new Text(avg.toString()));

        avg = 0.0;
        for(Map.Entry<Text, Double> entry: avgStartLoudHash.entrySet()) {
            avg += entry.getValue();
        }
        avg = avg/avgStartLoudHash.size();
        needWrite.put(new Text("7ls"), new Text(avg.toString()));

        avg = 0.0;
        for(Map.Entry<Text, Integer> entry: avgNumSegsHash.entrySet()) {
            avg += entry.getValue();
        }
        avg = avg/avgNumSegsHash.size();
        needWrite.put(new Text("7a"), new Text(avg.toString()));
    }
}
