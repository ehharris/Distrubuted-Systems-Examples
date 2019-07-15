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
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

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
            //Q1. Which artist has the most songs in the data set?
            Q1Map(delimData);
            //Q2. Which artist’s songs are the loudest on average?
            Q2Map(delimData);
            //Q3. What is the song with the highest hotttnesss (popularity) score?
            Q3Map(delimData);
            //Q4. Dataset needed already retrived from Q2 & Analysis/Mapper2
            //Q5. Dataset needed already retrived from Q2 & Q4
            //Q6. Dataset retrived from Analysis/Mapper2
        }
    }

    //produces artID, '1(a/s)'
    private void Q1Map(String[] delimData) {
        String artID = delim1(delimData[3]);
        String artName = delim1(delimData[7]);
        if (!artID.equals("artist_id") && !artID.isEmpty() &&
            !artName.equals("artist_name") && !artName.isEmpty()) {
            //'1a' is added to signify which question it's relevant to.
            String artID1 = "1a" + artID;
            needWrite.put(new Text(artID1), new Text("1"));
            //'1s' is added to signify which question it's relevant to.
            String artID2 = "1s" + artID;
            needWrite.put(new Text(artID2), new Text(artName));
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

    //produces song_id, song_title
    private void Q3Map(String[] delimData) {
        String song_id = delim1(delimData[8]);
        String song_title = delim1(delimData[9]);
        if (!song_id.isEmpty() && !song_title.isEmpty() &&
                !song_id.equals("song_id") && !song_title.equals("title")) {
            //'3m' is added for Q3 and metadata file
            song_id = "3m" + song_id;
            needWrite.put(new Text(song_id), new Text(song_title));
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
