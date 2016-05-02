package org.test.spark.examples.TransR




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

object test {
  
  def norm( x:Array[Double]) : Double = {
     var sum = 0.0;
     for (i <- 0 to x.length-1){
       sum = sum + x(i)*x(i)
     }
     return Math.sqrt(sum)
  }
   
  def get_left_energy(a:Array[Double],b:Array[Double]):Double = {
     val n = a.length
     var sum = 0.0
      for (i <- 0 to n-1){
       sum = sum + Math.pow(a(i)-b(i),2)
     }
     return sum
  }
   
  def get_right_energy(a:Array[Double],b:Array[Double]):Double = {
     val n = a.length
     var sum = 0.0
      for (i <- 0 to n-1){
       sum = sum + Math.pow(a(i)+b(i),2)
     }
     return sum
  }
   
  def vecadd(a:Array[Double],b:Array[Double]):Array[Double] = {
     val n = a.length
     var x = new Array[Double](n);
     for (i <- 0 to n-1){
       x(i) = a(i)+b(i)
     }
     return x
  }
   
   
  def vecsub(a:Array[Double],b:Array[Double]):Array[Double] = {
     val n = a.length
     var x = new Array[Double](n);
     for (i <- 0 to n-1){
       x(i) = a(i)-b(i)
     }
     return x
  }
  
  
  def main(args: Array[String]) {
      val strt = ((new Date).getTime).toDouble
      val inputPath = args(0)
      val dim = args(1).toInt
      val numPartitions = args(2).toInt
      val sc = new SparkContext(new SparkConf().set("spark.hadoop.validateOutputSpecs", "false"))
      val entity = sc.broadcast(sc.textFile(inputPath + "/ent").map(x => {
           val ids = x.split(",")
           (ids(0).toInt, ids(1).split("\\s+").map(_.toDouble))
      }).collectAsMap())
      val entity_num = entity.value.size
      val relation = sc.broadcast(sc.textFile(inputPath + "/rel").map(x => {
           val ids = x.split(",")
           (ids(0).toInt, ids(1).split("\\s+").map(_.toDouble))
      }).collectAsMap())
      val relation_num = relation.value.size
      val testRdd = sc.textFile(inputPath+"/test_data",numPartitions)
      val extract_data = testRdd.map(elem => {
      val ids = elem.split("\\s+")
      val e1 = ids(0).toInt
      val e2 = ids(1).toInt
      val rel = ids(2).toInt
      val w_e1 = entity.value.getOrElse(e1, null)
      val w_r = relation.value.getOrElse(rel, null)
      val w_e2 = entity.value.getOrElse(e2, null)
      val w_left = vecadd(w_e1,w_r)
      val w_right = vecsub(w_r,w_e2)
      var leftrank = 0
      var mydiff = get_left_energy(w_left,w_e2)
      for(i <- 0 to entity_num-1){
        if(i!=e2){
          val w_e = entity.value.getOrElse(i, null)
          val diff = get_left_energy(w_left,w_e)
          if(diff < mydiff) leftrank += 1
        }
      }
      var lefthits10 = 0
      if(leftrank < 10)
          lefthits10 = 1
      var rightrank = 0
      mydiff = get_right_energy(w_e1,w_right)
      for(i <- 0 to entity_num-1){
         if(i!=e1){
          val w_e = entity.value.getOrElse(i, null)
          val diff = get_right_energy(w_e,w_right)
          if(diff < mydiff) rightrank += 1
         }
      }
        
      var righthits10 = 0
      if(rightrank < 10)
          righthits10 = 1
      
          
     (leftrank,rightrank,lefthits10,righthits10)
     })
      
     val leftmeanrank = extract_data.map(elem => elem._1).mean()
     val rightmeanrank = extract_data.map(elem => elem._2).mean()
     val lefthits10 = extract_data.map(elem => elem._3).mean()
     val righthits10 = extract_data.map(elem => elem._4).mean()
     println("Left: MeanRank = %5.8f; Hits@10 = %5.8f;".format(leftmeanrank,lefthits10))
     println("Right: MeanRank = %5.8f; Hits@10 = %5.8f;".format(rightmeanrank,righthits10))
     val end = ((new Date).getTime).toDouble
     println("time = %5.8f;".format((end-strt)/1000))
   }
}