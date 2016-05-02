package org.test.spark.examples


import scala.util.control._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{SparseVector}
import splash.core.{SplashConf,ParametrizedRDD,SharedVariableSet,LocalVariableSet}
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.collection.immutable
import java.util.Date

object preprocess {
  
  
    def get_replica(avg:Double,max:Double,min:Double,x:Double):Int = {
      if(avg <= x) return 1
      else if( (avg+min)/2 <= x) return 2
      else if( (avg+3*min)/4 <= x) return 4
      else return 8
    }
  
   def main(args: Array[String]) {

     
      val strt = ((new Date).getTime).toDouble

      val path = args(0)
      val numPartitions = args(1).toInt
      
      val sc = new SparkContext(new SparkConf().set("spark.hadoop.validateOutputSpecs", "false"))
      
      val entity2id = sc.broadcast(sc.textFile(path + "/entity2id.txt")
          .map(x => (x.split("\\s+")(0), x.split("\\s+")(1).toInt)).collectAsMap())
      
      val entity_num = entity2id.value.count(p => true)
      println("entities = " +  entity_num)
          
          
      val relation2id = sc.broadcast(sc.textFile(path + "/relation2id.txt")
          .map(x => (x.split("\\s+")(0), x.split("\\s+")(1).toInt)).collectAsMap())
      
      
      val testRdd = sc.textFile(path + "/test.txt",numPartitions).map(sample => {
        val ids = sample.split("\\s+")
        val e1 =  entity2id.value.getOrElse(ids(0), -1)
        val e2 =  entity2id.value.getOrElse(ids(1), -1)
        val rel = relation2id.value.getOrElse(ids(2), -1)
        val newsample = e1 + " " + e2 + " " + rel
        newsample
      })
      testRdd.saveAsTextFile(path + "/test_data")
      
      

      
      //training part
      val trainRdd = sc.textFile(path + "/train.txt",numPartitions).map(sample => {
        val ids = sample.split("\\s+")
        val e1 =  entity2id.value.getOrElse(ids(0), -1)
        val e2 =  entity2id.value.getOrElse(ids(1), -1)
        val rel = relation2id.value.getOrElse(ids(2), -1)
        val newsample = e1 + " " + e2 + " " + rel
        newsample
      })
      
      val dataSize = trainRdd.count()
    
     
      
      //ranking based on density
      val entity_density = trainRdd.flatMap(sample => {
      val ids = sample.split("\\s+")
      val e1 =  ids(0).toInt
      val e2 =  ids(1).toInt
      Array((e1,1),(e2,1))
      }).reduceByKey(_ + _).map(elem => (elem._1,elem._2*50.0/dataSize))
      
      
      val densities = entity_density.map(elem => elem._2)
      
      val min_density = densities.min()
      println("min density = " +  min_density)
      
      val max_density = densities.max()
      println("max density = " +  max_density)
      
      val avg_density = densities.mean()
      println("max density = " +  avg_density)
      
    
      val entity_replica = sc.broadcast(entity_density.map(elem => 
        (elem._1,get_replica(avg_density,max_density,min_density,elem._2))).collectAsMap())
      

			
      val leftcorruptRdd = trainRdd.map(sample => {
          val ids = sample.split("\\s+")
          ((ids(0).toInt,ids(2).toInt),ids(1).toInt)
      }).groupByKey().flatMap( item => {
          
          val e1 = item._1._1
          val rel = item._1._2
          val replica = entity_replica.value.getOrElse(e1, -1)
          val corrupted = scala.collection.mutable.Set( (0 to entity_num-1 toArray) :_* )
          corrupted.remove(e1)  //remove e1
          
          var iter = item._2.toIterator 
          while(iter.hasNext) corrupted.remove(iter.next())  //remove all e2
         
          val rand = new scala.util.Random(rel)
          val sz = corrupted.size
          val samples = scala.collection.mutable.Set[(Int,Int,Int,Int,Int,Int)]()
          var e2 = 0
          var l = 0
          
          iter = item._2.toIterator
          val corruptedlist = corrupted.toList
          while(iter.hasNext){
            e2 = iter.next()
            for(i <- 0 to replica-1 ){
              l = corruptedlist(rand.nextInt(sz))
              samples.add((e1,e2,rel,e1,l,rel))
            }
          }
          
          samples.toList
        
      })
    	
    

      
      
      
			
      val rightcorruptRdd = trainRdd.map(sample => {
          val ids = sample.split("\\s+")
          ((ids(1).toInt,ids(2).toInt),ids(0).toInt)
      }).groupByKey().flatMap( item => {
          
          val e2 = item._1._1
          val rel = item._1._2
          val replica = entity_replica.value.getOrElse(e2, -1)
          val corrupted = scala.collection.mutable.Set( (0 to entity_num-1 toArray) :_* )
          corrupted.remove(e2)  //remove e1
          
          var iter = item._2.toIterator 
          while(iter.hasNext) corrupted.remove(iter.next())  //remove all e2
         
          val rand = new scala.util.Random(rel)
          val sz = corrupted.size
          val samples = scala.collection.mutable.Set[(Int,Int,Int,Int,Int,Int)]()
          var e1 = 0
          var h = 0
          
          iter = item._2.toIterator
          val corruptedlist = corrupted.toList
          while(iter.hasNext){
            e1 = iter.next()
            for(i <- 0 to replica-1 ){
              h = corruptedlist(rand.nextInt(sz))
              samples.add((e1,e2,rel,h,e2,rel))
            }
          }
          
          samples.toList
        
      })

      
      val corruptRdd = leftcorruptRdd.union(rightcorruptRdd)
      
      corruptRdd.map(elem => elem._1.toString() + " " + elem._2 + " " +elem._3 + " " +elem._4 + " " +elem._5 + " " +elem._6)saveAsTextFile(path + "/training_data")
        
      
      val end = ((new Date).getTime).toDouble
      println("time = %5.8f;".format((end-strt)/1000))
      
      
   }
      
}
