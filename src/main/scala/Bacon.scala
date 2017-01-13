/* Bacon.scala */
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListener, SparkListenerJobStart}
import org.apache.spark.storage.StorageLevel

object Bacon {
  final val KevinBacon = "Bacon, Kevin (I)" // This is how Kevin Bacon's name appears in the input file for male actors
  final val r = """\*?\(.*?\)""".r
  val compressRDDs = true
  // SparkListener must log its output in file sparkLog.txt
  val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))
  var kevinHash =0.0
  
  def getTimeStamp(): String =
    {
      return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
    }

  // my sustom function to set storage level and to better effect the rdd compress 
  // rdds will be cached with compression if storage level is serialized otherwise I will use normal storage level to save time on cost of more memory for caching  
  
  def getStorageLevel(): org.apache.spark.storage.StorageLevel ={
    return  StorageLevel.MEMORY_ONLY_SER
    // StorageLevel.MEMORY_ONLY // use any of the storage levle
    // this function can be used to set differenct storage level for caching just chaning at single function.  
    // if (compressRDDs)
    //  return StorageLevel.MEMORY_ONLY_SER
    //  else 
    //   return StorageLevel.MEMORY_ONLY
   }

  def measureDistance(hash: Long): Int =
    {
      var distance = 10
      if(hash == kevinHash){
       distance = 0;
      }
      return distance
    }

  def endingIndex(p: String): Int = {
   // helper function to get the year in movie title  
    val tokens = p.split(" ").filter(s => s.length() > 5)
      .filter(x => x.startsWith("(")).filter(x => x.endsWith(")"))
      .filter(x => x.slice(1, 5).matches("[0-9]+"))
    tokens.head.indexOf(")")
  }

  def movieeName(line: String): String = {
    // helping function to remove post string after year (2015) 
    val substr = r.findFirstMatchIn(line).mkString
    line.substring(0, line.indexOfSlice(substr)) + substr
  }

  def main(args: Array[String]) {
    
  // val cores = 4.toString() //args(0) // Number of cores to use
   val cores = args(0) // Number of cores to use
   val inputFileM = args(1) // Path of input file for male actors
   val inputFileF = args(2) // Path of input file for female actors (actresses)

    var t0 = System.currentTimeMillis
    // val inputFileM = "/home/najeeb/spark/wksp/Assignment2/v1/input/mod_actors.list"
    // val inputFileF = "/home/najeeb/spark/wksp/Assignment2/v1/input/mod_actresses.list"
//       val inputFileF = "/home/najeeb/spark/wksp/Assignment2/v1/input/short_actors.list"

    val conf = new SparkConf().setAppName("Kevin Bacon app")
    conf.setMaster("local[" + cores + "]")
    conf.set("spark.cores.max", cores)
    conf.set("spark.kryo.referenceTracking", "true")
    
    
    if(compressRDDs){
      conf.set("spark.rdd.compress", "true") 
    }

    val sc = new SparkContext(conf)
    // registering new spark listener and overrided its method statge competed 
    sc.addSparkListener(new SparkListener() {
       
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
        val rdds = stageCompleted.stageInfo.rddInfos
        rdds.foreach { rdd =>
          if (rdd.isCached){
            bw.write(Bacon.getTimeStamp() + rdd.name + " :  memsize = " + rdd.memSize + ", diskSize = " + rdd.diskSize + ", numPartitions = " + rdd.numPartitions +"\n")
          }else{
            bw.write(Bacon.getTimeStamp() + rdd.name + " processed!\n") 
          } 
            
         }
       bw.flush() 
      }

    });
  
    // instant of Configuration  class of Hadop file readinf  
    val config = new Configuration
    // configring custom deliminator as required in the input file
    config.set("textinputformat.record.delimiter", "\n\n")

    //--> STEP::1
    // In step, most of the code is to clean the data,
    //In this step I putting comments where I made update for application logic LIKE male/female flag
    
    val maleFile = sc.newAPIHadoopFile(inputFileM, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], config).map(_._2.toString)
    maleFile.setName("rdd_Fileread_Male")
    
    val femaleFile = sc.newAPIHadoopFile(inputFileF, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], config).map(_._2.toString)
    femaleFile.setName("rdd_Fileread_Female") 
    //   I made rdd of Array(string, string, string)
    // then I took first index as key (actor name) and rest of the elements as value (list of movies)
    
    val maleRDD = maleFile.map(_.split("\t")).map(arr => (arr(0), arr.slice(1, arr.length)))
    maleRDD.setName("rdd_maleMoviesArray")
    
    val femaleRDD = femaleFile.map(_.split("\t")).map(arr => (arr(0), arr.slice(1, arr.length)))
    femaleRDD.setName("rdd_maleMoviesArray")
    
    // Filtering out TV series  for male 
    val moviesInMale = maleRDD.map(x => {
      val list = x._2.filter(_.length() > 0).filter(x => x.trim().indexOf("\"") != 0)
      (x._1, list)
    }).filter(f => f._2.length > 0)
    moviesInMale.setName("rdd_Filter_TVSeries_in_Male")
    
    // Filtering out TV series  for female 
    
    val moviesInFemale = femaleRDD.map(x => {
      val list = x._2.filter(_.length() > 0).filter(x => x.trim().indexOf("\"") != 0)
      (x._1, list)
    }).filter(f => f._2.length > 0)
    moviesInMale.setName("rdd_Filter_TVSeries_in_Female") 
    
    // filtering out movies before 2011 
    val moviesInDecadMale = moviesInMale.map(x => {
      val list = x._2.filter(p => {
        val tokens = p.split(" ").filter(s => s.length() > 5)
          .filter(x => x.startsWith("(")).filter(x => x.endsWith(")"))
          .filter(x => x.slice(1, 5).matches("[0-9]+"))
        if (tokens.length > 0) {
          val year = tokens.head.slice(1, 5).toInt 
          year > 2010 && year < 2017
        } else
          false
      })
      (x._1, list.filter(_.length > 0).map(p => { movieeName(p) }))
    }).filter(f => f._2.length > 0).map(x => (x._1, (1, x._2))) // here I have included flag for gender (male= 1 & Female=2 ) 
    moviesInDecadMale.setName("rdd_ActiveMalActorsIn_CurrentDecad")
    
    
    val moviesInDecadFemale = moviesInFemale.map(x => {
      val list = x._2.filter(p => {
        val tokens = p.split(" ").filter(s => s.length() > 5)
          .filter(x => x.startsWith("(")).filter(x => x.endsWith(")"))
          .filter(x => x.slice(1, 5).matches("[0-9]+"))
        if (tokens.length > 0) {
          val year = tokens.head.slice(1, 5).toInt
          year > 2010 && year < 2017
        } else
          false
      })
      (x._1, list.filter(_.length > 0).map(p => { movieeName(p) }))
    }).filter(f => f._2.length > 0).map(x => (x._1, (2, x._2)))  // here I have included flag for gender (male= 1 & Female=2 )
    moviesInDecadFemale.setName("rdd_ActiveFemalActorsIn_CurrentDecad")
    
    // moviesInDecadFemale -> (actorName, (1, Array<Movies>)), moviesInDecadFemale -> (actressName, (2, Array<Movies>))  
     // up till now I have filtered data and now I have rdds of male and female actos with list of movies 
    
    //--> STEP::2
    // In step 2, I joined male female RDDs and replaced string actors names into numbers
    // taking union of both RDDs to make single RDD
    val mfGenderMovies = moviesInDecadMale.union(moviesInDecadFemale)  // (name, (gender, list<movies>))
    mfGenderMovies.setName("rdd_Male_Female_Union")
    
    //  indexRDD --> (name, LongHash) 
    val indexRDD = mfGenderMovies.keys.zipWithIndex().persist(getStorageLevel)  // Extracted the keys and made a assigned it Long ID, and now its hash table 
    indexRDD.setName("rdd_withHashing_Index")
    
    // Join this hash table with original RDD to extract requited attributes with hash as key  
    val hashRDD = indexRDD.join(mfGenderMovies).persist(getStorageLevel)  //(name, (hashKey (gender, List<movies>)))
    hashRDD.setName("rdd_Male_Female_MoviesList_Gender")
    
    // lookupTBL is rdd that I will use to look for the names of actors/actress at distance 6, will use latter   
    val lookupTBL = hashRDD.map(x=> (x._2._1,( x._1, x._2._2._1) )) // (hash, ( name, gender ))
    lookupTBL.setName("rdd_Lookup")
    
    // Now making a rdd that will be precess further to execute algorithm
    val processingRDD = hashRDD.map(x=> (x._2._1, x._2._2._2))  // (hash, list<movies>)
    processingRDD.setName("rdd_Cleaned_with_hashing")
    
    // As now names are replaces with hashkeys, so getting hash key of Kevin Bacon to measure distance from that point  
    kevinHash = indexRDD.lookup(KevinBacon).head // it will return long value
    
    // flat mapped values to make actos vs movie in which he worked like  (a, m1) (a, m2) (b, m2) ...
    val actorVsMovies = processingRDD.flatMapValues(x => x)
    actorVsMovies.setName("rdd_actorVsMovies")
    
    // (movie, List<actors>)  movie as key and list of all the actors who worked in that movie 
    val movieeActorList = actorVsMovies.map(x => (x._2, x._1)).groupByKey.map(x => (x._1.toString, x._2.toList)).persist(getStorageLevel) //.map{x=> (x._1,x._2.toList) } // or combinie after that using sme
    movieeActorList.setName("rdd_moviesActor_list")
    
//    movieeActorList.persist(getStorageLevel) 
    // getting count of movies to display on output file 
    val nbMovies = movieeActorList.count()
    
    // (actor, List<coloborators>)  // now Flatmapped above rdd and made 2 way cartesian of acots who worked with each other, to make every1 to others collaborator  
    val actorClobAll = movieeActorList.flatMap(rdd => rdd._2.flatMap { x => rdd._2.map(y => (x, y)) }.toSeq.filter(pair => pair._1 != pair._2))
    actorClobAll.setName("rdd_actors_withAll_colaborators")
    
    // (actor, List<coloborators>) DISTINCT, few actors became coloborators from two different movies so removing duplicate entries from the list of collaborators  
    val actorClobUniq = actorClobAll.groupByKey.map(x => (x._1, x._2.toList.distinct)).persist(getStorageLevel)
    actorClobUniq.setName("rdd_Actorswiht_uniq_colaborators")
    
    //-> STEP::3 Assigning distance to each actor infinity while kevin to 0
    // this rdd will be used in all iterations, gradually it will update the distance of each actors starting from infinity (10 in this case)
    var distanceToKevin = actorClobUniq.map { rdd => (rdd._1, measureDistance(rdd._1)) }
    distanceToKevin.setName("rdd_gloabalToHold_allDistance") 
    
    // <->Performance Improvement, this step to assing 1 distance from their collaborators is manged emplicity without making extra map, it reduced that cost 
    //   val individaulDistance = actorClobUniq.map(rdd => (rdd._1, (1, rdd._2))).cache() // can be outside
    // ->If Spark is running with workers at distributed location below braodcast improves performace, it sends whole data onces that is being used during 6 iterations // but in 1 system its not very notable 
    //   val braodcasted = sc.broadcast(individaulDistance)
    
    var round = 1
    while(round < 7){ // using while instead of for, as in scala for loops over integers are slower than While  
        
        // measure intermediate distance at start of round, and adding 1 for all the collaborators to reach through that acots 
                            // (hashID, (distance of Actors to reach kevin +1 to reach him, List<actors>  ))  
        val interMediateDistance = actorClobUniq.join(distanceToKevin).map(x=> (x._1, (x._2._2+1, x._2._1)))
              
        // this operation expanding all the actors with possible distance to kevin, I will reduce them after picking minimum possible distance 
        val polarizedRDD = interMediateDistance.flatMap(x=> {
             var list = collection.mutable.Seq.empty[(Long, Int)]
             val dist = x._2._1
             val p1 = (x._1, dist-1)
             list = list :+p1 
             //-> performance Improvement, execute if distance is less than exectly equals to round, because only these actors can provide shortest path to their collaborators in this round  
             if (dist == round){
               list = list++(x._2._2.map(v => (v, dist) ).toSeq).toList
              } 
          list
          })      
        
       polarizedRDD.setName("rdd_polarized")   
       // reducing the suppressed RDD with shortest distance    
       distanceToKevin = polarizedRDD.reduceByKey((x, y)=> Math.min(x, y) )
       // reducebyKey key is Transformation to but I need the result in next iteration so, taking 1 to materializ the RDD, Take(1) is less expensive than first, I checked 
       distanceToKevin.take(1)       
       round +=1;  // updating round  
    }
   
    //-> STEP::4
    // Now distanceToKevin contains distance of all actos with their hash code 
    
    // joing with lookup RDD to get actors names and gender with distance calculated in above loop 
                                               //( name     gender       distance)
    val nameDistGender = lookupTBL.join(distanceToKevin).map(x=> (x._2._1._1, (x._2._1._2, x._2._2)))
    nameDistGender.setName("rdd_with_distance_gender_name")
    
    
    val updatedMaleRDD = nameDistGender.filter(x=> x._2._1 ==1).persist(getStorageLevel)   
    updatedMaleRDD.setName("rdd_male_distance_count")
    
    val updatedFemaleRDD = nameDistGender.filter(x=> x._2._1 ==2).persist(getStorageLevel) // getting count of all females 
    updatedMaleRDD.setName("rdd_Female_distance_count")
    
    val nbmale = updatedMaleRDD.count  // count of male actors 
    val nbfemale = updatedFemaleRDD.count // count of female actors  
    
    var maleStats = new Array[Long](6)
    var femaleStats = new Array[Long](6)
    
    // calculating count count for distance 1 to 6 both for male and female in that array 
    for(i <-1 to 6){
      maleStats(i-1) = updatedMaleRDD.filter(x=> x._2._2 == i).count()
      femaleStats(i-1) = updatedFemaleRDD.filter(x=> x._2._2 == i).count()
    }
    
   
    //-> STEP::5
    // Wrtting output to the file
    var totalActor = nbmale + nbfemale
    var malePercent = nbmale.toFloat / totalActor.toFloat * 100
    var femalePercent = nbfemale.toFloat / totalActor.toFloat * 100
    var bw2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("actors.txt"), "UTF-8"))
    var line = "Total number of actors = " + totalActor + ", out of which " + nbmale + " (" + malePercent + "%) are males while " + nbfemale + " (" + femalePercent + "%) are females.\n"
    bw2.write(line)
    line = "Total number of movies = " + nbMovies + "\n\n"
    bw2.write(line)

   
    println("Filter MAle : "  + nbmale)
    println("Filter FeMAle :"  + nbfemale)
   
    //stat 2
    for(i<-1 to 6){
       var maleCount = maleStats(i-1)
       var femaleCount = femaleStats(i-1)   
       var malepercent =  maleCount.toFloat/nbmale *100
       var femalepercent =  femaleCount.toFloat/nbfemale *100
     
       line = "There are "+maleCount+" ("+malepercent+"%) actors and "+femaleCount+" ("+femalepercent+"%) actresses at distance "+i+"\n\n"  
       bw2.write(line)
    }
    
  
    //type 3
    var sum = maleStats.sum+femaleStats.sum
    var ratio = sum.toLong / totalActor.toFloat
    line = "Total number of actors from distance 1 to 6 = "+sum+", ratio = "+ratio+"\n"
    bw2.write(line)
    sum =  maleStats.sum
    ratio = sum.toLong/nbmale.toFloat
    line = "Total number of male actors from distance 1 to 6 = "+sum+", ratio = "+ratio+"\n"
    bw2.write(line)
    sum =  femaleStats.sum
    ratio = sum.toLong/nbfemale.toFloat
    line = "Total number of female actors (actresses) from distance 1 to 6 = "+sum+", ratio = "+ratio+"\n\n"
    bw2.write(line)
    line ="List of male actors at distance 6:\n"
    bw2.write(line)

    
    // getting the names of actors and actress at distance 6 and collected them into array
    val maleAt6 = updatedMaleRDD.filter(x=>x._2._2== 6).map(x=> x._1).collect()
    val femaleAt6 = updatedMaleRDD.filter(x=>x._2._2 == 6).map(x=> x._1).collect()

    // writting names of male at 6 distance 
    var i = 1;
    maleAt6.foreach{actor=>
      bw2.write(i+". "+actor+"\n")
      i+=1
    }
    
    // writting names of females at 6 distance 
    bw2.write("\n")
    line = "List of female actors (actresses) at distance 6:\n"
    bw2.write(line)  
    i = 1;
    femaleAt6.foreach{actor=>
      bw2.write(i+". "+actor+"\n")
       i+=1
    }
    
    bw2.flush() 
    bw2.close()
   
    // Comment these two lines if you want to see more verbose messages from Spark
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    println("Starting my project ..........................")

    // Add your main code here

    sc.stop()
    bw.close()

    val et = (System.currentTimeMillis - t0) / 1000
    println("{Time taken = %d mins %d secs}".format(et / 60, et % 60))
  }
}