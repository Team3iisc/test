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
object init {
   val LOG = LoggerFactory.getLogger(getClass) 
    
   def normal(x:Double, mu:Double, sigma:Double ) : Double = {
       return 1.0/Math.sqrt(2*Math.PI)/sigma*Math.exp(-1*(x-mu)*(x-mu)/(2*sigma*sigma))
   }
   
   def randn( mu:Double, s:Double, min:Double, max:Double,seed:Int ) : Double = {
      var X = 0.0
      var Y = 0.0
      var dScope = 0.0
      val r = new scala.util.Random(seed)
      do{
        X = min + r.nextDouble()*(max-min)
        Y = normal(X,mu,s)
        dScope = r.nextDouble()*normal(mu,mu,s)
      }while( dScope > Y )
      return X
   }
   
   def norm( x:Array[Double]) : Double = {
     var sum = 0.0;
     for (i <- 0 to x.length-1){
       sum = sum + x(i)*x(i)
     }
     return Math.sqrt(sum)
   }

    
   def main(args: Array[String]) {
      val strt = ((new Date).getTime).toDouble
      val inputPath = args(0)
      val dim = args(1).toInt
      val numPartitions = args(2).toInt
      // Load data from file
      val sc = new SparkContext(new SparkConf().set("spark.hadoop.validateOutputSpecs", "false"))
      val relation_num = sc.textFile(inputPath + "/relation2id.txt").count().toInt  
      val entity_num   = sc.textFile(inputPath + "/entity2id.txt").count().toInt
      val entity       = 0 to (entity_num-1) toList
      val relation     = 0 to (relation_num-1) toList
      val entityrdd    = sc.parallelize(entity,numPartitions).mapPartitions(iter => {
          val mylist   = iter.toList
          mylist.map( id => {
          var vec      = new Array[Double](dim)
          var dev      = 6/Math.sqrt(dim)
          var vecnorm  = 0.0  
          for(ii <- 0 to dim-1){
           vec(ii) = randn(0,1.0/dim,-dev,dev,(id+ii+3))
          }
          vecnorm = norm(vec)
          for(ii <- 0 to dim-1){
           vec(ii) = vec(ii)/vecnorm
          }
          (id) + "," + vec.mkString(" ")  
          }).iterator
       })
      
      entityrdd.saveAsTextFile(inputPath+"/ent")
      
      val relationrdd = sc.parallelize(relation,numPartitions).mapPartitions(iter => {
          val mylist      = iter.toList
          mylist.map( id => {
          var vec       = new Array[Double](dim)
          var dev       = 6/Math.sqrt(dim)
          var vecnorm   = 0.0  
          for(ii <- 0 to dim-1){
            vec(ii)     = randn(0,1.0/dim,-dev,dev,(id+ii+3))
          }
          vecnorm = norm(vec)
          for(ii <- 0 to dim-1){
            vec(ii) = vec(ii)/vecnorm
           }
          (id) + "," + vec.mkString(" ")  
          }).iterator
      })
      relationrdd.saveAsTextFile(inputPath+"/rel")
      
    for(i <- 0 to relation_num-1){
      val projectionrdd = sc.parallelize(relation,numPartitions).mapPartitions(iter => {
          val mylist      = iter.toList
          mylist.map( id => {
          var vec       = new Array[Double](dim)
          var vecnorm   = 0.0  
          for(ii <- 0 to dim-1){
            if(id==ii)
              vec(ii)     = 1.0
            else  
              vec(ii)     = 0.0 
          }
          vecnorm = norm(vec)
          for(ii <- 0 to dim-1){
            vec(ii) = vec(ii)/vecnorm
           }
          (id) + "," + vec.mkString(" ")  
          }).iterator
      })
      projectionrdd.saveAsTextFile(inputPath+"/A"+i)
    }  
      val end = ((new Date).getTime).toDouble
      println("time = %5.8f;".format((end-strt)/1000))
     }
}