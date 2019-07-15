import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import java.sql.{DriverManager, Connection}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.BufferedOutputStream

//Author: Eli Harris (ehharris)
object SteamAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SteamAnalysis").getOrCreate()
    val sc = SparkContext.getOrCreate()
    Class.forName("com.mysql.cj.jdbc.Driver")
    import spark.implicits._


    //Connects to mySql database where the file structure is currently
    //Using Antonio's Database since Eli's needs to stay intact for another test
    val url = "jdbc:mysql://faure:3306/anthos?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val mysql_props = new java.util.Properties
    mysql_props.setProperty("user","anthos")
    //830655911
    mysql_props.setProperty("password","830655911")
    mysql_props.setProperty("driver","com.mysql.cj.jdbc.Driver")
    
    val user = "anthos"
    val password = "830655911"
    val driver = "com.mysql.cj.jdbc.Driver"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, user, password)
    val statement = connection.createStatement()

    //Creates DF's for all tables in dataset
    //This also partitions the query requests manually as it's not currently possible to repartition
    //this kind of data like you normally would in spark
    //Note: you may still run into issues if you have more than 10-15 executor processes
    
    
    //App_ID_Info
    val AppID_Result = statement.executeQuery("select min(appid), max(appid) from App_ID_Info")
    AppID_Result.next()
    val AppID_min = AppID_Result.getString(1).toLong
    val AppID_max = AppID_Result.getString(2).toLong
    val AppID_numPartitions = (AppID_max - AppID_min) / 5000 + 1
    
    val App_ID_Info_DF = spark.read.format("jdbc").option("url", url).option("driver", driver).option("lowerBound", AppID_min).option("upperBound", AppID_max).option("numPartitions", AppID_numPartitions).option("partitionColumn", "appid").option("dbtable", "App_ID_Info").option("user", user).option("password", password).load()
    
    //Games_2
    val Games2_Result = statement.executeQuery("select min(steamid), max(steamid) from Games_2")
    Games2_Result.next()
    val Games2_min = Games2_Result.getString(1).toLong
    val Games2_max = Games2_Result.getString(2).toLong
    val Games2_numPartitions = (Games2_max - Games2_min) / 6000 + 1
    
    val Games_2_DF = spark.read.format("jdbc").option("url", url).option("driver", driver).option("lowerBound", Games2_min).option("upperBound", Games2_max).option("numPartitions", Games2_numPartitions).option("partitionColumn", "steamid").option("dbtable", "Games_2").option("user", user).option("password", password).load()
    
    //Player_Summaries
    val PlayerSum_Result = statement.executeQuery("select min(steamid), max(steamid) from Player_Summaries")
    PlayerSum_Result.next()
    val PlayerSum_min = PlayerSum_Result.getString(1).toLong
    val PlayerSum_max = PlayerSum_Result.getString(2).toLong
    val PlayerSum_numPartitions = (PlayerSum_max - PlayerSum_min) / 6000 + 1
    
    val PlayerSum_DF = spark.read.format("jdbc").option("url", url).option("driver", driver).option("lowerBound", PlayerSum_min).option("upperBound", PlayerSum_max).option("numPartitions", PlayerSum_numPartitions).option("partitionColumn", "steamid").option("dbtable", "Player_Summaries").option("user", user).option("password", password).load()


    //Data Analysis "Methods"

    //1. Avg "current" price of a game
    val Game_Info_DF = App_ID_Info_DF.filter($"Type"==="Game")
    val Game_InfoasAvgPrice = Game_Info_DF.as("AvgPrice")
    val onea = Game_InfoasAvgPrice.select(avg($"Price"))
    //Optional call to show avg price w/o free games
    val Expensive_Info_DF = Game_Info_DF.filter($"Price">="0")
    val oneb = Expensive_Info_DF.select(avg($"Price"))

    //TODO: Def Needs Testing
    //2. Avg playtime per game
    //Games_2
    //1390.81489
    
    //avg user playtime + 2.* 4. 
    //dataframe_mysql.sqlContext.sql("select * from names").collect.foreach(println)
    val Game_Info_Games_2_DF = Game_Info_DF.join(Games_2_DF,Seq("appid"), "fullouter")
    val Game_Info_Games_2_DF2 = Game_Info_Games_2_DF.filter($"Type"=!="null")
    val Game_InfoasAvgPlayTime = Game_Info_Games_2_DF2.as("AvgPlayTime")
    val two = Game_Info_Games_2_DF.select(avg($"playtime_forever"))
    
    //3. Avg time users spend playing games
    val test = Games_2_DF.groupBy("steamid").sum("playtime_forever")
    val three = test.select(avg($"sum(playtime_forever)"))

    //4. Avg # of games in a users library
    val Games_Per_User = Games_2_DF.groupBy("steamid").count()
    val four = Games_Per_User.select(avg($"count"))

    //5. Avg % of games never played/games played
    val Games_Never_Played = Game_Info_Games_2_DF.filter($"playtime_forever"<"1").groupBy("steamid").count()
    val Altered_Games_Never_Played = Games_Never_Played.withColumnRenamed("count","neverPlayedCount")
    val Games_Never_Played_Per_User = Altered_Games_Never_Played.join(Games_Per_User,Seq("steamid"), "fullouter")
    val Games_Never_Played_Per_User_Percent = Games_Never_Played_Per_User.withColumn("Percent", $"neverPlayedCount" / $"count")
    val five = Games_Never_Played_Per_User_Percent.select(avg($"Percent"))

    //6. Avg cost of library = 1. * 4.
    //This doesn't need a query since we already have the results

    //7. Avg acct. age (player_summaries.dateretrived - timecreated)
    val Player_Summaries_DF2 = PlayerSum_DF.withColumn("drSec", $"dateretrieved".cast("long"))
    val Player_Summaries_DF3 = Player_Summaries_DF2.withColumn("tcSec", $"timecreated".cast("long"))
    val Player_Summaries_DF4 = Player_Summaries_DF3.withColumn("Account_Age", $"drSec" - $"tcSec")
    val Player_Summaries_DF5 = Player_Summaries_DF4.filter($"Account_Age">"604800")
    
    val seven = Player_Summaries_DF5.select(avg($"Account_Age"))

    //8. Avg cost per month/day/year = 6. / 7.
    //This doesn't need a query since we already have the results

    //9. ANS Adjusted avg cost per month/day/year = (6. * .5) / 7.
    //This doesn't need a query since we already have the results

    val path = new Path("hdfs://jackson:30300/cs455/tp/results2.txt")
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    conf.setInt("dfs.blocksize", 16 * 1024 * 1024) // 16 MB HDFS Block Sizes
    val fs = path.getFileSystem(conf)
    if(fs.exists(path))
      fs.delete(path, true)
    val out = new BufferedOutputStream(fs.create(path))
    val txt = "1a. " + onea.collectAsList().toString() + "\n" +
          "1b. " + oneb.collectAsList().toString() + "\n" +
          "2. " + two.collectAsList().toString() + "\n" +
          "3. " + three.collectAsList().toString() + "\n" +
          "4. " + four.collectAsList().toString() + "\n" +
          "5. " + five.collectAsList().toString() + "\n" +
          "7. " + seven.collectAsList().toString()
    out.write(txt.getBytes("UTF-8"))
    out.flush()
    out.close()
    fs.close()


  }
}
