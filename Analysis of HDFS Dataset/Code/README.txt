ehharris
ehharris@rams.colostate.edu
CS455 HW3

Welcome to my HW3! Everything is pretty self explanatory based on the naming scheme but I'll go more in depth here.

To Run:
Setup hadoop exactly like how the instructions are on the course website/piazza.
In the code directory you'll want to do a 'gradle build' before anything.
The jar file will end up in the build/libs folder, should be named "hw3.hadoop.jar"
To start the program do something like
    "$HADOOP_HOME/bin/hadoop jar path/to/jar cs455.hadoop.hw3.Job1 /path/to/data(contains folders analysis and metadata /path/to/Q1-6output /path/to/Q7-9output"
That should get you all set up if your .bashrc file is setup properly as per the course website.
All answers to the questions should automatically go into 1 file in each of your output folders.
    Don't worry, it's clearly labeled what's what.

Class Rundown:
Job1.java: Driver for the whole operation, it creates a single job and manages the Mappers/Reducers.
Mapper1.java: This mapper is in charge of analysing the 'metadata' files. Relevant Questions: 1,2,3,
Mapper2.java: This mapper is in charge of analysing the 'analysis' files. Relevant Questions: 2,3,4,5,6,
Mapper3.java: This mapper is in charge of analysing the 'metadata' files. Relevant Questions: 8,9,
Mapper4.java: This mapper is in charge of analysing the 'analysis' files. Relevant Questions: 7,9,
Reducer1.java: Q1-6 reducing takes place here. Each method is named after the question it hopes to answer.
Reducer2.java: Q7-9 reducing takes place here. Each method is named after the question it hopes to answer.

Question Explanations:
Q1-6: Should be self explanatory but I'd be happy to explain them and their answers at the grading interview.
Q7:     
        My approach to this is to get the mean values (rather than median like Q5) of the attributes asked for
    all songs. My logic behind this is that this way you get a look at what a song on average looks/sounds like for almost all
    of it attributes. Also, before all the songs get sent the reducer, I'm calculating the averages in my cleanup class for Mapper 
    (cleanupHashMaps()to cut down on memory used by the reducer. This acutally makes it so I can run the tasks for each question
    all in one job, however I'm keeping it two seprate jobs for now because they don't take too long and its better for organization. 

Q8:     
        The most generic artist is probably someone who would make a song close to what's described in Q7. Just plain and average.
    So, what I'm looking for is the artist who falls closest to these means, and furthest away for uniqueness. To get this average,
    I add together all the attributes looked at for Q8 after "normalizing" the values by either multipling or dividing by 10
    so the fall in the range of 100-1000. This way, a value that's only usually between 0-1 can have an equal presence as
    something that's usually 1,000,000 (exagerrating but you never know). This assgins each artist a value (100-3000) in Mapper
    and based on that is how I determine the most unique (lowest) and the most generic (highest).

Q9:     
        My idea for this question is to what happens if a (poorly trained/coded) computer tried to create the "hotttesstt"
    song on earth. The way it works is that it analyzes the top hotttest songs from Q3 and takes a little bit from each one.
    It's name is make up of the first character of each hottt artists name same with the title of the song. From here the
    computer decides to go a bit bonkers though. It then sums up the rest of the attributes of every hottt song together and
    makes that it's attributes. For example, tempo may normally be 120 but for the computer, it thinks the best tempo is every
    single hottt songs tempo's added together. After all, if it's good enough to be hot then the more hottness you add to it
    the better the song you get right? So for every value it either takes the first char in a string or adds the full value to
    total if it's any kind of number. For genre values it won't add a value to it again if it's already present.

Q10:
        Sadly, I didn't set aside enough time to get Q10 done but I did have a pretty cool idea and I'd still like to share it
    with y'all. What I wanted to do is link this database up with the discogs database (https://www.discogs.com/) which also
    catalogs songs but it includes prices for how much albums and whatnot are selling for. I was going to import some relevant
    data from discogs so I could look up the albums that songs were in and get their current selling price and highest selling
    price/dateItSold and compare those values to the "hotttnesss" scores to see if "hotttnesss" of a song/album had any effect
    on it's price. Since the discogs data is more up to date than the millionsong data, it might not be entirely accurrate but
    I still think it'd hold some relevance as some songs are timeless and maintain their popularity over the years.

Dataset From: https://labrosa.ee.columbia.edu/millionsong/
Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere.
The Million Song Dataset. In Proceedings of the 12th International Society
for Music Information Retrieval Conference (ISMIR 2011), 2011.
