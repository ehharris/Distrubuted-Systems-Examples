package cs455.hadoop.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.Math.*;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Reducer2 extends Reducer<Text, Text, Text, Text> {

    //Output LinkedHashmap to Minimize Context Switch
    private LinkedHashMap<Text, Text> output  = new LinkedHashMap<>();
    //Used so certain values are rounded to 3 decimal places for readablility
    private DecimalFormat df = new DecimalFormat("#.####");
    //Q2: This is still here because the values are handy
    //Contains SongID and ArtName
    private HashMap<String, String> songIDArtName = new HashMap<>();
    //Contains ArtID and Avg Loudness
    private HashMap<String, Double> songIDLoud = new HashMap<>();
    //Q7
    //Contains SongID and SegmentStartTimes
    private HashMap<String, Double> songIDavgSegStart = new HashMap<>();
    //Contains SongID and SegmentPitchTimes
    private HashMap<String, Double> songIDavgSegPitch = new HashMap<>();
    //Contains SongID and SegmentTibreTimes
    private HashMap<String, Double> songIDavgTimbre = new HashMap<>();
    //Contains SongID and SegmentLoudnessMaxLevels
    private HashMap<String, Double> songIDavgLoudMax = new HashMap<>();
    //Contains SongID and SegmentLoudnessMaxTimes
    private HashMap<String, Double> songIDavgLoudMaxT = new HashMap<>();
    //Contains SongID and SegmentLoudnessStartTimes
    private HashMap<String, Double> songIDavgLoudStart = new HashMap<>();
    //Contains SongID and Number of Segments in the Song
    private HashMap<String, Double> songIDavgNumSegs = new HashMap<>();
    //Q8
    //Contains SongID and Artist Familiarity
    private HashMap<String, String> songIDArtFam = new HashMap<>();
    //Contains SongID and Number of Similar Artists
    private HashMap<String, String> songIDNumSimArt = new HashMap<>();
    //Contains SongID and The Sum of The Artists Frequent Terms
    private HashMap<String, String> songIDArtTermFreqSum = new HashMap<>();
    //Contains SongID and Uniqueness Score
    //Uniqueness is determined by normalizing Familiarity, Similar Artists,
    //and Artists Frequent Terms Sum to 'val <= 1000 && val >= 100' so there's
    //a max score of 3000 and min of 100.
    private HashMap<String, Double> ArtNameGenericUnique = new HashMap<>();
    //Q9
    //All these HashMaps ONLY contain values from the "hotttest" artists and songs
    //Contains SongID and TimeSignature
    private HashMap<String, Integer> songIDTimeSig = new HashMap<>();
    //Contains SongID and Mode
    private HashMap<String, Integer> songIDMode = new HashMap<>();
    //Contains SongID and Key
    private HashMap<String, Integer> songIDKey = new HashMap<>();
    //Contains ArtistID and an Artists most popular term (whichever term
    //is deemed the most frequent
    private HashMap<String, String> artNamePopTerm = new HashMap<>();
    //Contains SongID and Tempo
    private HashMap<String, Double> songIDTempo = new HashMap<>();
    //Contains HOT SongID and artName
    private HashMap<String, String> HOTsongIDartName = new HashMap<>();
    //Contains SongID and Duration
    private HashMap<String, Double> songIDDuration = new HashMap<>();
    //Contains SongID and Loudness
    private HashMap<String, Double> songIDLoudness = new HashMap<>();
    //Contains SongID and FadeInStopTime
    private HashMap<String, Double> songIDFadeIn = new HashMap<>();
    //Contains SongID and FadeOutStartTime
    private HashMap<String, Double> songIDFadeOut = new HashMap<>();
    //Contains SongID and Title
    private HashMap<String, String> songIDTitle = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        //Sets rounding mode
        df.setRoundingMode(RoundingMode.CEILING);
        String realKey = key.toString().substring(1);
        //Tasks change based on which question the values are relevant to
        switch (key.charAt(0)){
            case '2':
                Q2reduce(realKey, values);
                break;
            case '7':
                Q7reduce(realKey, values);
                break;
            case '8':
                Q8reduce(realKey, values);
                break;
            case '9':
                Q9reduce(realKey, values);
                break;
            default:
                break;
        }
//        Q10. Come up with an interesting question of your own to answer. This question should be more
//        complex than Q7, Q8 or Q9. Answer it.
        //Q10reduce(key, values);
    }

    //Input = '(m/a)'+song_id, artName/loudness
    private void Q2reduce(String song_id, Iterable<Text> values){
        if(song_id.charAt(0) == 'm'){
            song_id = song_id.substring(1);
            for (Text artName: values){
                songIDArtName.put(song_id, artName.toString());
            }
        }else if(song_id.charAt(0) == 'a'){
            double count = 0;
            double totalLoud = 0;
            song_id = song_id.substring(1);
            for (Text loudness: values) {
                totalLoud = Double.parseDouble(loudness.toString()) + totalLoud;
                count++;
            }
            double avgLoud = totalLoud/count;
            songIDLoud.put(song_id, avgLoud);
        }
    }
    
    //Input = '(s/p/ti/lm/lmt/ls/te)',
    //start/pitch/timbre/maxLoud/maxLoudTime/startLoud/tempo
    private void Q7reduce(String song_id, Iterable<Text> values){
         switch (song_id.charAt(0)){
            case 's':
                for (Text segStart : values) {
                    songIDavgSegStart.put(song_id, (Double.valueOf(segStart.toString())));
                }
                break;
            case 'p':
                for (Text segPitch : values) {
                    songIDavgSegPitch.put(song_id, (Double.valueOf(segPitch.toString())));
                }
                break;
            case 't':
                for (Text segTimbre : values) {
                    songIDavgTimbre.put(song_id, (Double.valueOf(segTimbre.toString())));
                }

                break;
            case 'l':
                song_id = song_id.substring(1);
                if(song_id.charAt(0) == 's'){
                    for (Text segLoudStart : values) {
                        songIDavgLoudStart.put(song_id, (Double.valueOf(segLoudStart.toString())));
                    }
                }else if(song_id.charAt(0) == 'm'){
                    song_id = song_id.substring(1);
                    if(song_id.length() == 1) {
                        for (Text segLoudMaxTime : values) {
                            songIDavgLoudMaxT.put(song_id, (Double.valueOf(segLoudMaxTime.toString())));
                        }
                    }else{
                        for (Text segLoudMax : values) {
                            songIDavgLoudMax.put(song_id, (Double.valueOf(segLoudMax.toString())));
                        }
                    }
                }
                break;
             case 'a':
                 for (Text numSegs : values) {
                     songIDavgNumSegs.put(song_id, (Double.valueOf(numSegs.toString())));
                 }
             default:
                break;
        }
    }

    //Input = '(f/s/t)'+song_id, artFamiliarity/numOfSimilarArts/artTermsFreqSum
    private void Q8reduce(String song_id, Iterable<Text> values){
        switch (song_id.charAt(0)){
            case 'f':
                song_id = song_id.substring(1);
                for(Text artFam : values) {
                    songIDArtFam.put(song_id, artFam.toString());
                }
                break;
            case 's':
                song_id = song_id.substring(1);
                for(Text numSimArt : values) {
                    songIDNumSimArt.put(song_id, numSimArt.toString());
                }
                break;
            case 't':
                song_id = song_id.substring(1);
                for(Text artTermFreqSum : values) {
                    songIDArtTermFreqSum.put(song_id, artTermFreqSum.toString());
                }
                break;
            case 'u':
                String artName = song_id.substring(1);
                for(Text artTermFreqSum : values) {
                    ArtNameGenericUnique.put(artName, Double.parseDouble(artTermFreqSum.toString()));
                }
                break;
            default:
                break;
        }
    }

    //Input = 'p'+artID, popTerm / (s/m/k/t/h/d/l/fi/fo)'+song_id, timeSig/mode/key/tempo
    private void Q9reduce(String song_id, Iterable<Text> values){
        switch (song_id.charAt(0)){
            case 'p':
                String artName = song_id.substring(1);
                for(Text popTerm : values) {
                    artNamePopTerm.put(artName, popTerm.toString());
                }
                break;
            case 'j':
                song_id = song_id.substring(1);
                for(Text title : values) {
                    songIDTitle.put(song_id, title.toString());
                }
                break;
            case 's':
                song_id = song_id.substring(1);
                for(Text timeSig : values) {
                    songIDTimeSig.put(song_id, Integer.parseInt(timeSig.toString()));
                }
                break;
            case 'm':
                song_id = song_id.substring(1);
                for(Text mode : values) {
                    songIDMode.put(song_id, Integer.parseInt(mode.toString()));
                }
                break;
            case 'k':
                song_id = song_id.substring(1);
                for(Text key : values) {
                    songIDKey.put(song_id, Integer.parseInt(key.toString()));
                }
                break;
            case 't':
                song_id = song_id.substring(1);
                for(Text tempo : values) {
                    songIDTempo.put(song_id, Double.parseDouble(tempo.toString()));
                }
            case 'h':
                song_id = song_id.substring(1);
                for(Text HOTartName : values) {
                    HOTsongIDartName.put(song_id, HOTartName.toString());
                }
                break;
            case 'd':
                song_id = song_id.substring(1);
                for(Text duration : values) {
                    songIDDuration.put(song_id, Double.parseDouble(duration.toString()));
                }
            case 'l':
                song_id = song_id.substring(1);
                for(Text loudness : values) {
                    songIDLoudness.put(song_id, Double.parseDouble(loudness.toString()));
                }
                break;
            case 'f':
                song_id = song_id.substring(1);
                if(song_id.charAt(0) == 'i'){
                    song_id = song_id.substring(1);
                    for(Text fadeIn : values) {
                        songIDFadeIn.put(song_id, Double.parseDouble(fadeIn.toString()));
                    }
                }
                if(song_id.charAt(0) == 'o'){
                    song_id = song_id.substring(1);
                    for(Text fadeOut : values) {
                        songIDFadeOut.put(song_id, Double.parseDouble(fadeOut.toString()));
                    }
                }

                break;
            default:
                break;
        }
    }
    /**
     * This does all the context writing once all reducing has finished.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Q7cleanup();
        Q8cleanup();
        Q9cleanup();
        FinalCleanup(context);
    }

    //output: "Question 7: " "Song Segment Averages"
    //"Average Segment Data"
    //"Average Segment Pitch"
    //"Average Segment Timbre"
    //"Average Segment Max Loudness"
    //"Average Segment Time of Max Loudness"
    //"Average Segment Loudness Start Time"
    private void Q7cleanup(){
        //Array to hold avg values
        Double[] avgs = new Double[]{0.0,0.0,0.0,0.0,0.0,0.0,0.0};
        Double count = 0.0;
        //Get all values
        Iterator it = songIDavgSegStart.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry) it.next();
            String realKey = val.getKey();
            Double realVal = val.getValue();
            if (realKey != null) {
                if (realVal != null) {avgs[0] += realVal;}
                if (songIDavgSegPitch.containsKey(realKey)) {avgs[1] += songIDavgSegPitch.get(realKey);}
                if (songIDavgTimbre.containsKey(realKey)) {avgs[2] += songIDavgTimbre.get(realKey);}
                if (songIDavgLoudMax.containsKey(realKey) ) {avgs[3] += songIDavgLoudMax.get(realKey);}
                if (songIDavgLoudMaxT.containsKey(realKey) ) {avgs[4] += songIDavgLoudMaxT.get(realKey);}
                if (songIDavgLoudStart.containsKey(realKey)) {avgs[5] += songIDavgLoudStart.get(realKey);}
                if (songIDavgNumSegs.containsKey(realKey)) {avgs[6] += songIDavgNumSegs.get(realKey);}
                count++;
            }
        }

        //Avg them
        for(int i = 0; i < avgs.length; i++){
            avgs[i] = avgs[i]/count;
        }

        output.put(new Text("\nQuestion 7: "), new Text("Song Segment Averages"));
        output.put(new Text("Average Segment Start Time"), new Text(df.format(avgs[0])));
        output.put(new Text("Average Segment Pitch"), new Text(df.format(avgs[1])));
        output.put(new Text("Average Segment Timbre"), new Text(df.format(avgs[2])));
        output.put(new Text("Average Max Loudness Of A Segment"), new Text(df.format(avgs[3])));
        output.put(new Text("Average Time of Max Loudness Occurs In A Segment"), new Text(df.format(avgs[4])));
        output.put(new Text("Average Loudness At The Start Of A Segment"), new Text(df.format(avgs[5])));
        output.put(new Text("Average Number of Segments"), new Text(df.format(avgs[6])));
    }

    //output:  "Question 8: " "Most Generic/Unique Artists"
    //"Disclaimer: " "There may be multiple artists with the same values but only one is shown per category"
    //"Artist with Highest Familiarity" "artName    Value"
    //"Artist with Lowest Familiarity" "artName Value"
    //"Artist with Highest Amount of Similar Artists" "artName  Value"
    //"Artist with Lowest Amount of Similar Artists" "artName   Value"
    //"Artist with Most Common Artist Terms" "artName   Value"
    //"Artist with Least Common Artist Terms" "artName  Value"
    //"Calculated Most Generic Artist" "artName Value"
    //"Calculated Most Unique Artist" "artName  Value"
    private void Q8cleanup(){
        //Calculate all values, separated for readability
        String[] famStats = Q8calc(songIDArtFam.entrySet());
        String[] simStats = Q8calc(songIDNumSimArt.entrySet());
        String[] artTermStats = Q8calc(songIDArtTermFreqSum.entrySet());
        String[] genericUnique = calcGenericUnique(ArtNameGenericUnique.entrySet());

        output.put(new Text("\nQuestion 8:"), new Text("Most Generic/Unique Artists"));
        output.put(new Text("Disclaimer: "), new Text("There may be multiple artists with" +
                " the same values but only one is shown per category\n That's how award shows work so that's how we're doing it"));
        output.put(new Text("Artist with Highest Familiarity"),
                new Text(famStats[0]));
        output.put(new Text("Artist with Lowest Familiarity"),
                new Text(famStats[1]));
        output.put(new Text("Artist with Highest Amount of Similar Artists"),
                new Text(simStats[0]));
        output.put(new Text("Artist with Lowest Amount of Similar Artists"),
                new Text(simStats[1]));
        output.put(new Text("Artist with Most Common Artist Terms"),
                new Text(artTermStats[0]));
        output.put(new Text("Artist with Least Common Artist Terms"),
                new Text(artTermStats[1]));
        output.put(new Text("Calculated Most Generic Artist"),
                new Text(genericUnique[0]));
        output.put(new Text("Calculated Most Unique Artist"),
                new Text(genericUnique[1]));
    }

    //output:  "Question 9: " "Song With The Highest Hotttnesss"
    //"Song Title:  "
    //"Artist Name: "
    //"Tempo: "
    //"Time Signature: "
    //"Danceability: "
    //"Duration: "
    //"Mode: "
    //"Energy: "
    //"Key: "
    //"Loudness: "
    //"Fade in stop: "
    //"Fade out start: "
    //"Artist Terms: "
    private void Q9cleanup(){
        //Our "artist" is going to grab a little bit of everything from the top hottt songs to make an uber song.
        //My methodology for this,7,8&10 is located in my README

        //Everything is already sorted by hotttness = 1 to ensure the dankest of beats
       
        String title = "";
        String name = "";
        Double tempo = 0.0;
        Integer timeSig = 0;
        //No danceability scores so it's set to over 9000
        Double danceability = 9001.0;
        Double duration = 0.0;
        Integer mode = 0;
        //No energry scores so it's set to over 9000
        Double energy = 9001.0;
        Integer key = 0;
        Double loudness = 0.0;
        Double fadeInStop = 0.0;
        Double fadeOutStart = 0.0;
        ArrayList<String> terms = new ArrayList<>();
        int count = 0;
        
        //Getting the values
        //Takes first char of every title
        Iterator it = songIDTitle.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, String> val = (Map.Entry)it.next();
            String sTitle = val.getValue();
            if(!sTitle.equals("")){
                title += sTitle.charAt(0);
            }
        }
        //Takes first char of every artName
        it = artNamePopTerm.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, String> val = (Map.Entry)it.next();
            String artName = val.getKey();;
            name += artName.charAt(0);
        }
        //Adds all the tempos together
        it = songIDTempo.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            tempo += val.getValue();
        }
        //Adds all the timeSigs together
        it = songIDTimeSig.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Integer> val = (Map.Entry)it.next();
            timeSig += val.getValue();
        }    
        //Adds all the durations together
        it = songIDDuration.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            duration += val.getValue();
        }    
        //Adds all the modes together
        it = songIDMode.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Integer> val = (Map.Entry)it.next();
            mode += val.getValue();
        }   
        //Adds all the keys together
        it = songIDKey.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Integer> val = (Map.Entry)it.next();
            key += val.getValue();
        } 
        //Adds all the loud together
        it = songIDLoud.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            loudness += val.getValue();
        }
        //Adds all the fade in stop times together
        it = songIDFadeIn.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            fadeInStop += val.getValue();
        }
        //Adds all the fade out stop times  together
        it = songIDFadeOut.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            fadeOutStart += val.getValue();
        }
         //Adds each hot artists most frequent term to the list
        it = artNamePopTerm.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, String> val = (Map.Entry)it.next();
            if(!terms.contains(val.getValue())){
                terms.add(val.getValue());
            }
        }
        Double realFadeInStop = Math.abs(fadeInStop - duration);
        Double realFadeOutStart = Math.abs(fadeOutStart - duration);
    
        output.put(new Text("\nQuestion 9: "), new Text("Computer makes a hot and spicy song"));
        output.put(new Text("Song Title:  "), new Text(title));
        output.put(new Text("Artist Name: "), new Text(name));
        output.put(new Text("Song Hotttnesss:  "), new Text("1 Googleplex"));
        output.put(new Text("Tempo: "), new Text(tempo.toString()));
        output.put(new Text("Time Signature: "), new Text(timeSig.toString()));
        output.put(new Text("Danceability: "), new Text(danceability.toString()));
        output.put(new Text("Duration: "), new Text(duration.toString()));
        output.put(new Text("Mode: "), new Text(mode.toString()));
        output.put(new Text("Energy: "), new Text(energy.toString()));
        output.put(new Text("Key: "), new Text(key.toString()));
        output.put(new Text("Loudness: "), new Text(loudness.toString()));
        output.put(new Text("Fade in stop: "), new Text(realFadeInStop.toString()));
        output.put(new Text("Fade out start: "), new Text(realFadeOutStart.toString()));
        output.put(new Text("Artist Terms: "), new Text(terms.toString()));
    }

    private void FinalCleanup(Context context) throws IOException, InterruptedException{
        Iterator it = output.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Text, Text> val = (Map.Entry) it.next();
            context.write(val.getKey(), val.getValue());
        }
    }

    //Q8. produces high and low fam/simArt/ArtTermFreq, format: "artName\tValue"
    private String[] Q8calc(Iterable values){
        String[] ans = new String[2];
        double maxValue = Double.MIN_VALUE;
        double minValue = Double.MAX_VALUE;
        Iterator it = values.iterator();

        //Setting default ans in case no values found
        ans[0] = "No Stats Found";
        ans[1] = "No Stats Found";

        while (it.hasNext()){
            Map.Entry<String, String> val = (Map.Entry)it.next();
            if(songIDArtName.containsKey(val.getKey())){
                String artName = songIDArtName.get(val.getKey());
                double value = Double.parseDouble(val.getValue());
                if (value > maxValue) {
                    maxValue = value;
                    ans[0] = artName + "\t" + value;
                } else if (value < minValue) {
                    minValue = value;
                    ans[1] = artName + "\t" + minValue;
                }
            }
        }
        return ans;
    }

    //Q8. Gets high and low from ArtNameGenericUnique, format: "artName\tValue"
    //High Number is Generic, Low is Unique
    private String[] calcGenericUnique(Iterable values){
        String[] ans = new String[2];
        double maxValue = Double.MIN_VALUE;
        double minValue = Double.MAX_VALUE;
        Iterator it = values.iterator();

        //Setting default ans in case no values found
        ans[0] = "No Stats Found";
        ans[1] = "No Stats Found";

        while (it.hasNext()) {
            Map.Entry<String, Double> val = (Map.Entry) it.next();
            if(val.getValue() > maxValue){
                maxValue = val.getValue();
                ans[0] = val.getKey() + "\t" + val.getValue();
            }
            if(val.getValue() < minValue){
                minValue = val.getValue();
                ans[1] = val.getKey() + "\t" + val.getValue();
            }
        }
        return ans;
    }

}
