package cs455.hadoop.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    //Output LinkedHashmap to Minimize Context Switch
    private LinkedHashMap<Text, Text> output  = new LinkedHashMap<>();
    //Used so certain values are rounded to 3 decimal places for readablility
    private DecimalFormat df = new DecimalFormat("#.####");
    //Q1
    //Contains ArtID and #of songs
    private HashMap<String, Integer> artIDSongCount = new HashMap<>();
    //Contains SongID and ArtID
    private HashMap<String, String> artIDArtName = new HashMap<>();
    //Q2
    //Contains SongID and ArtName
    private HashMap<String, String> songIDArtName = new HashMap<>();
    //Contains ArtID and Avg Loudness
    private HashMap<String, Double> songIDLoud = new HashMap<>();
    //Q3
    //Contains SongID and Hotttnesss
    private HashMap<String, Double> songIDHot = new HashMap<>();
    //Contains SongID and Title
    private HashMap<String, String> songIDTitle = new HashMap<>();
    //Contains SongIDs of Hotttessst Songs
    private HashMap<String, String> hotttessstSongIDs = new HashMap<>();
    //Q4
    //Contains SongID and TotalFadeTime
    private HashMap<String, Double> songIDFade = new HashMap<>();
    //Contains SongID and Duration
    private HashMap<String, Double> songIDDuration = new HashMap<>();
    //Q5 Uses datasets already established.
    //Q6
    //Contains SongID and Danceability
    private HashMap<String, Double> songIDDance = new HashMap<>();
    //Contains SongID and Energy
    private HashMap<String, Double> songIDEnergy = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        //Sets rounding mode
        df.setRoundingMode(RoundingMode.CEILING);
        String realKey = key.toString().substring(1);
        //Tasks change based on which question the values are relevant to
        switch (key.charAt(0)){
            case '1':
                Q1reduce(realKey, values);
                break;
            case '2':
                Q2reduce(realKey, values);
                break;
            case '3':
                Q3reduce(realKey, values);
                break;
            case '4':
                Q4reduce(realKey, values);
                break;
            case '6':
                Q6reduce(realKey, values);
                break;
            default:
                break;
        }
    }

    //Input = '(a/s)'+artID, songCount/artName
    private void Q1reduce(String realKey, Iterable<Text> values){
        if(realKey.charAt(0) == 'a'){
            String artID = realKey.substring(1);
            int songCount = 0;
            // calculate the total count
            for(Text val : values){
                songCount += Integer.parseInt(val.toString());
            }
            if(!artIDSongCount.containsKey(artID)) {
                artIDSongCount.put(artID, songCount);
            } else{
                int oldCount = artIDSongCount.get(artID);
                artIDSongCount.remove(artID);
                artIDSongCount.put(artID, (oldCount+songCount));
            }
        } else if(realKey.charAt(0) == 's'){
            String artID = realKey.substring(1);
            for (Text artName: values){
                artIDArtName.put(artID, artName.toString());
            }
        }
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

    //Input = '(m/a)'+song_id, song_title/hotttnesss
    private void Q3reduce(String song_id, Iterable<Text> values){
        if(song_id.charAt(0) == 'm'){
            song_id = song_id.substring(1);
            for(Text title: values){
                songIDTitle.put(song_id, title.toString());
            }
        }else if(song_id.charAt(0) == 'a'){
            song_id = song_id.substring(1);
            for(Text hot: values){
                songIDHot.put(song_id, Double.parseDouble(hot.toString()));
            }
        }
    }

    //Input = song_id, total fade time
    private void Q4reduce(String song_id, Iterable<Text> values){
        if(song_id.charAt(0) == 'f') {
            song_id = song_id.substring(1);
            for (Text title : values) {
                songIDFade.put(song_id, Double.parseDouble(title.toString()));
            }
        } else if(song_id.charAt(0) == 'd'){
            song_id = song_id.substring(1);
            for (Text title : values) {
                songIDDuration.put(song_id, Double.parseDouble(title.toString()));
            }
        }
    }

    //Input = '(d/e)'+song_id, dancability/engergy
    private void Q6reduce(String song_id, Iterable<Text> values){
        if(song_id.charAt(0) == 'd') {
            song_id = song_id.substring(1);
            for (Text title : values) {
                songIDDance.put(song_id, Double.parseDouble(title.toString()));
            }
        } else if(song_id.charAt(0) == 'e'){
            song_id = song_id.substring(1);
            for (Text title : values) {
                songIDEnergy.put(song_id, Double.parseDouble(title.toString()));
            }
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
        Q1cleanup();
        Q2cleanup();
        Q3cleanup();
        Q4cleanup();
        Q5cleanup();
        Q6cleanup();
        FinalCleanup(context);
    }

    //output:  "Question 1: "
    //"artName" "numOfSongs"
    //If there are multiple artists with the highest song count it prints all of them
    private void Q1cleanup(){
        ArrayList<String> maxKeys = new ArrayList<>();
        int maxValue = 0;
        Iterator it = artIDSongCount.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Integer> val = (Map.Entry)it.next();
            if (val.getValue() > maxValue && artIDArtName.containsKey(val.getKey())) {
                maxKeys.clear();
                String artName = artIDArtName.get(val.getKey());
                maxKeys.add(artName);
                maxValue = val.getValue();
            }
            else if(val.getValue() == maxValue)
            {
                maxKeys.add(artIDArtName.get(val.getKey()));
            }
        }
        String out = "Question 1: \n";
        output.put(new Text(out), new Text(""));
        for(String key: maxKeys){
            output.put(new Text(key), new Text(Integer.toString(maxValue)));
        }
    }

    //output:  "Question 2: "
    //"artName" "avgLoudness"
    //If there are multiple artists with the highest avg loudness it prints all of them
    private void Q2cleanup(){
        ArrayList<String> maxKeys = new ArrayList<>();
        double maxValue = Double.MIN_VALUE;
        Iterator it = songIDLoud.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            if (val.getValue() > maxValue && songIDArtName.containsKey(val.getKey())) {
                maxKeys.clear();
                maxKeys.add(songIDArtName.get(val.getKey()));
                maxValue = val.getValue();
            }
            else if(val.getValue() == maxValue) {
                maxKeys.add(val.getKey());
            }
        }
        output.put(new Text("\nQuestion 2: "), new Text(""));
        for(String key: maxKeys){
            output.put(new Text(key), new Text(df.format(maxValue)));
        }
    }

    //output:  "Question 3: "
    //"song_title" "hotttnesss"
    //If there are multiple songs with the highest hotttnesss it prints all of them
    private void Q3cleanup(){
        double maxValue = Double.MIN_VALUE;
        Iterator it = songIDHot.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            if (val.getValue() > maxValue && songIDTitle.containsKey(val.getKey())) {
                hotttessstSongIDs.clear();
                hotttessstSongIDs.put(val.getKey(), "");
                maxValue = val.getValue();
            }
            else if(val.getValue() == maxValue && songIDTitle.containsKey(val.getKey()))
            {
                hotttessstSongIDs.put(val.getKey(), "");
            }
        }

        output.put(new Text("\nQuestion 3: "), new Text(""));
        output.put(new Text(hotttessstSongIDs.size() + " Songs Have Same Max Value Of " + maxValue + ", They Are Listed Below"),new Text(""));
        it = hotttessstSongIDs.entrySet().iterator();
        String song_titles = "";
        while (it.hasNext()) {
            Map.Entry<String, String> val = (Map.Entry) it.next();
            song_titles += songIDTitle.get(val.getKey()) + ", ";
        }
        output.put(new Text(song_titles), new Text(df.format(maxValue)));

    }

    //output: "Question 4: "
    //"artName" "fadeTimeAllSongs(to the 3rd decimal)"
    //If there are multiple artists with the highest fade time it prints all of them
    private void Q4cleanup(){
        ArrayList<String> maxKeys = new ArrayList<>();
        Iterator it = songIDFade.entrySet().iterator();
        double maxFade = 0;
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            if (val.getValue() > maxFade && songIDArtName.containsKey(val.getKey())) {
                maxKeys.clear();
                String artName = songIDArtName.get(val.getKey());
                maxKeys.add(artName);
                maxFade = val.getValue();
            }
            else if(val.getValue() == maxFade && songIDArtName.containsKey(val.getKey())){
                maxKeys.add(songIDArtName.get(val.getKey()));
            }
        }
        //Rounds the fading value for easier reading
        String out = "\nQuestion 4: ";
        output.put(new Text(out), new Text(""));
        if(maxKeys.size() > 1) {
            output.put(new Text(maxKeys.size() + " Artists Have Same Max Value, They Are Listed Below"),new Text(""));
            for (String key : maxKeys) {
                output.put(new Text(key), new Text(df.format(maxFade)));
            }
        }
        else if (maxKeys.size() == 1){
            String song = maxKeys.get(0);
            output.put(new Text(song), new Text(df.format(maxFade)));
        }
    }

    //output: "Question 5: "
    //"Longest Song(s)" "longSong"
    //"Shortest Song(s)" "shortSong"
    //"Medianest Song(s)" "medianSongTime"
    //If there are multiple artists with the highest [insert value here] it prints all of them
    private void Q5cleanup(){
        ArrayList<String> longSongs = new ArrayList<>();
        double longSongTime = Double.MIN_VALUE;
        ArrayList<String> shortSongs = new ArrayList<>();
        double shortSongTime = Double.MAX_VALUE;
        ArrayList<String> medianSongs = new ArrayList<>();
        double medianSongTime = calcMedianSongTime();

        Iterator it = songIDDuration.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Double> val = (Map.Entry)it.next();
            if (val.getValue() > longSongTime && songIDTitle.containsKey(val.getKey())) {
                longSongs.clear();
                String songTitle = songIDTitle.get(val.getKey());
                longSongs.add(songTitle);
                longSongTime = val.getValue();
            }
            else if(val.getValue() == longSongTime && songIDTitle.containsKey(val.getKey())){
                longSongs.add(songIDTitle.get(val.getKey()));
            }
            if (val.getValue() < shortSongTime && songIDTitle.containsKey(val.getKey())) {
                shortSongs.clear();
                String songTitle = songIDTitle.get(val.getKey());
                shortSongs.add(songTitle);
                shortSongTime = val.getValue();
            }
            else if(val.getValue() == shortSongTime && songIDTitle.containsKey(val.getKey())){
                shortSongs.add(songIDTitle.get(val.getKey()));
            }
            //For median length songs, there is a tolerance of .5 sec to be included.
            if ((val.getValue() == medianSongTime || (val.getValue()-medianSongTime < .5  &&
                    val.getValue()-medianSongTime > -.5)) && songIDTitle.containsKey(val.getKey())){
                medianSongs.add(songIDTitle.get(val.getKey()));
            }

        }

        //String for all the songs to go into
        String songs = "";
        output.put(new Text("\nQuestion 5: "), new Text(""));
        output.put(new Text("Longest Song(s)"), new Text(df.format(longSongTime)));
        for(String key: longSongs) {
            songs += key + ", ";
        }
        output.put(new Text(songs), new Text(""));
        songs = "";
        output.put(new Text("Shortest Song(s)"), new Text(df.format(shortSongTime)));
        for(String key: shortSongs) {
            songs += key + ", ";
        }
        output.put(new Text(songs), new Text(""));
        songs = "";
        output.put(new Text("Medianest Song(s)"),
                new Text(df.format(medianSongTime)));
        output.put(new Text("There are " + medianSongs.size() + "Median Songs within .5 sec of the median time"),
                new Text(df.format(medianSongTime)));
        for(String key: medianSongs) {
            songs += key + ", ";
        }
        output.put(new Text(songs), new Text(""));
    }

    //output: "Question 6: "
    //"Top 10 Danceable and Energetic Song(s):"
    //'Danceable and Energetic' Calculated by adding the 2 values together
    //If all songs are not rated/rated 0 no songs show up
    private void Q6cleanup(){
        String[] song_ids = new String[10];
        double[] songScores = new double[10];
        Iterator it = songIDEnergy.entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            Map.Entry<String, Double> val = (Map.Entry) it.next();
            if (songIDDance.containsKey(val.getKey()) && songIDTitle.containsKey(val.getKey())) {
                double energy = val.getValue();
                double dance = songIDDance.get(val.getKey());
                double score = energy + dance;
                //If score = 0 it doesn't get added
                if (score != 0 && score > songScores[9]) {
                    if (count < 10) {
                        songScores[count] = score;
                        song_ids[count] = val.getKey();
                        count++;
                    } else {
                        int superCount = 0;
                        for (double topScore : songScores) {
                            if (topScore < score) {
                                songScores[superCount] = score;
                                song_ids[superCount] = val.getKey();
                                break;
                            }
                            superCount++;
                        }
                    }
                }
            }
        }
        output.put(new Text("\nQuestion 6: "), new Text(""));
        output.put(new Text("Top 10 Danceable and Energetic Song(s):"), new Text(""));
        if(songScores[0] > 0){
            double songScore = songScores[0];
            count = 0;
            while (songScore > 0 && count < 10){
                output.put(new Text(songIDTitle.get(song_ids[count])), new Text(Double.toString(songScore)));
                songScore = songScores[count];
                count++;
            }
        }else{
            output.put(new Text("No songs were rated above a 0 in danceabililty or energy! What are the odds?!"), new Text(""));
        }
    }

    private void FinalCleanup(Context context) throws IOException, InterruptedException{
        Iterator it = output.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Text, Text> val = (Map.Entry) it.next();
            context.write(val.getKey(), val.getValue());
        }
    }

    //Calculates median time by sorting all song times and picking the song in the middle of the array
    private double calcMedianSongTime(){
        double[] songTimes = new double[songIDDuration.size()];
        Iterator it = songIDDuration.entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            Map.Entry<String, Double> val = (Map.Entry) it.next();
            songTimes[count] = val.getValue();
            count++;
        }
        Arrays.sort(songTimes);
        double medianSongTime = songTimes[songTimes.length/2];
        return medianSongTime;
    }

}
