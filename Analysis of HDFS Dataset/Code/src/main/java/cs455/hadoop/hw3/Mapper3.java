package cs455.hadoop.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {

    //Hashmap so data can all be written in cleanup to avoid context switches
    private HashMap<Text, Text> needWrite = new HashMap<>();

    /**
     * This mapper takes care of the metadata files and sorts/adds values by the question they're related to.
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
            //Q7. In Mapper4
            //Q8. Which artist is the most generic? Which artist is the most unique?
            Q8Map(delimData);
            //Q9. Imagine a song with a higher hotttnesss score than the song in your answer to Q3. List this
            //song’s tempo, time signature, danceability, duration, mode, energy, key, loudness, when it
            //stops fading in, when it starts fading out, and which terms describe the artist who made it.
            //Give both the song and the artist who made it unique names.
            Q9Map(delimData);
        }
    }

    //produces song_id, artName
    private void Q2Map(String[] delimData) {
        String song_id = delim1(delimData[8]);
        String artName = delim1(delimData[7]);
        if (!artName.equals("artist_name") && !artName.isEmpty() &&
                !song_id.isEmpty() && !song_id.equals("song_id")) {
            //'2m' is added for Q2 and metadata file
            song_id = "2m" + song_id;
            needWrite.put(new Text(song_id), new Text(artName));
        }
    }

    //produces song_id, artist_familiarity, numSimilarArt, artTermsFreqSum, uniquenessVal
    private void Q8Map(String[] delimData) {
        if (!delimData[8].isEmpty() && !delimData[8].equals("song_id") &&
                !delimData[1].isEmpty() && !delimData[1].equals("artist_familiarity") &&
                !delimData[10].isEmpty() && !delimData[10].equals("similar_artists") &&
                !delimData[7].isEmpty() && !delimData[7].equals("artist_name") &&
                !delimData[12].isEmpty() && !delimData[12].equals("artist_terms_freq")){
        String song_id = delim1(delimData[8]);
        String artName = delim1(delimData[7]);
        Double artFam = Double.parseDouble(delimData[1]);
        String[] simArt = delimData[10].split(" ");
        String[] artTerms = delimData[12].split(" ");
        int simArtNum = simArt.length;
        Double termFreq = 0.0;
        for(String term: artTerms) {
            termFreq += Double.parseDouble(term);
        }
        
        needWrite.put(new Text("8f" + song_id), new Text(artFam.toString()));
        needWrite.put(new Text("8s" + song_id), new Text(Integer.toString(simArtNum)));
        needWrite.put(new Text("8t" + song_id), new Text(termFreq.toString()));
        //This is the value that deteremines uniqueNess, based on art_fam, numberOfSimilarArt, artTermsFreqSum
        while(termFreq < 100){
            if(termFreq == 0){
                termFreq = 100.0;
            }else{
            termFreq = termFreq * 10.0;
            }
        }
        while(termFreq > 1000){
            termFreq = termFreq / 10.0;
        }
        while(simArtNum < 100){
            if(simArtNum == 0){
                simArtNum = 100;
            }else{
            simArtNum = simArtNum * 10;
            }
        }
        while(simArtNum > 1000){
            simArtNum = simArtNum / 10;
        }
        while(artFam < 100){
            if(artFam == 0){
                artFam = 100.0;
            }else{
            artFam = artFam * 10.0;
            }
        }
        while(artFam > 1000){
            artFam = artFam / 10.0;
        }
        Double uniqueNess = termFreq + simArtNum + artFam;
        needWrite.put(new Text("8u" + artName), new Text(uniqueNess.toString()));
        }
    }

    //produces artName, Most "popular term" (taken from terms_freq) only from hot artists
    //also produces songIDTitle
    private void Q9Map(String[] delimData){
        
        Double hotVal = 0.5;
        if (!delimData[9].isEmpty() && !delimData[9].equals("title") &&
            !delimData[8].isEmpty() && !delimData[8].equals("song_id")&&
            !delimData[2].isEmpty()){
             if(Double.parseDouble(delimData[2]) >= hotVal){
                hotVal = Double.parseDouble(delimData[2]);
                String title = delim1(delimData[9]);
                String song_id = delim1(delimData[8]);
                if(!title.equals("")){
                    title = title.charAt(0) + " ";
                    needWrite.put(new Text("9j" + song_id), new Text(title));
                }
            }
        }
        if (!delimData[7].isEmpty() && !delimData[7].equals("artist_name") &&
                !delimData[11].isEmpty() && !delimData[11].equals("artist_terms") &&
                !delimData[12].isEmpty() && !delimData[12].equals("artist_terms_freq") &&
                !delimData[2].isEmpty()){
            if(Double.parseDouble(delimData[2]) >= hotVal){
            hotVal = Double.parseDouble(delimData[2]);
            String artName = delim1(delimData[7]);
            String[] artTerms = delimData[11].split(" ");
            String[] artTermsFreq = delimData[12].split(" ");
            
            //default "popular" term
            String popTerm = "None found";
            double topTermFreq = 0;
            int arrayLength = artTerms.length;
            if ((artTerms.length - artTermsFreq.length) > 0){
                arrayLength = artTermsFreq.length;
            }
            for(int i = 0; i < arrayLength; i++){
                double freqVal = Double.parseDouble(artTermsFreq[i]);
                if(freqVal >= topTermFreq){
                    topTermFreq = freqVal;
                    popTerm = artTerms[i];
                }
            }
            needWrite.put(new Text("9p" + artName), new Text(popTerm));
            }
        }
    }

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
