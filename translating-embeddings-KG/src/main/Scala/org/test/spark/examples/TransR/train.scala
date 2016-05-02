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

object train {
   /*
   val LOG = LoggerFactory.getLogger(getClass) 
   def norm( x:Array[Double]) : Double = {
     var sum = 0.0;
     for (i <- 0 to x.length-1){
       sum = sum + x(i)*x(i)
     }
     return Math.sqrt(sum)
   }
   
   def get_energy(w_left:Array[Double],w_e2:Array[Double]):Double = {
     val n = w_left.length
     var sum = 0.0
      for (i <- 0 to n-1){
       sum = sum + Math.pow(w_left(i)-w_e2(i),2)
     }
     return sum
   }
   
   def vecadd(w_e1:Array[Double],w_r:Array[Double]):Array[Double] = {
     val n = w_e1.length
     var x = new Array[Double](n);
     for (i <- 0 to n-1){
       x(i) = w_r(i)+w_e1(i)
     }
     return x
   }
   
   def get_residual( e1:Array[Double],e2:Array[Double],r:Array[Double],Mr:Array[Array[Double]]) : Array[Double] = {
     val n = e1.length
     var x = new Array[Double](n);
     
     var dot1 = 0.0
     for (i <- 0 to n-1){
       dot1 = 0.0
       for(j <- 0 to n-1){
         e1(i) += (Mr(i)(j)*e1(j))
       }
     }
     
    var dot2 = 0.0
     for (i <- 0 to n-1){
       dot2 = 0.0
       for(j <- 0 to n-1){
         e2(i) += (Mr(i)(j)*e2(j))
       }
     }
     
     for (i <- 0 to n-1){
       x(i) = (e2(i)- e1(i) -r(i))
     }
     return x
   }
   
   def main(args: Array[String]) {
    
    var strt = ((new Date).getTime).toDouble
    val inputPath = args(0)
    val learningRate = args(1).toDouble
    val NumIteration = args(2).toInt
    // Load data from file
    val sc = new SparkContext(new SparkConf().set("spark.hadoop.validateOutputSpecs", "false"))
    val dim = args(3).toInt
    val margin = args(4).toDouble
    val dataperiteration = args(5).toDouble
    val numPartitions = args(6).toInt
    
    val entity = sc.textFile(inputPath + "/ent").map(x => {
       val ids = x.split(",")
       (ids(0).toInt, ids(1).split("\\s+").map(_.toDouble))
      }).collectAsMap()
    val entity_num = entity.size
    
    val relation = sc.textFile(inputPath + "/rel").map(x => {
      val ids = x.split(",")
      (ids(0).toInt, ids(1).split("\\s+").map(_.toDouble))
      }).collectAsMap()
    val relation_num = relation.size
    
    val A = new Array[Map[Int,Array[Double]]](relation_num)
    for(i <- 0 to relation_num-1){
       A(i) = sc.textFile(inputPath + "/A"+i).map(x => {
       val ids = x.split(",")
       (ids(0).toInt, ids(1).split("\\s+").map(_.toDouble))
       }).collectAsMap().toMap
    }
    
    var end = ((new Date).getTime).toDouble
    println("time for loading vectors = %5.8f;".format((end-strt)/1000))
    
    // Create parametrized RDD and declare shared array
    val paramRdd = new ParametrizedRDD(sc.textFile(inputPath + "/training_data",numPartitions)).reshuffle()
    strt = ((new Date).getTime).toDouble
    //declare variables
    paramRdd.foreachSharedVariable( sharedVar => {
      for(i <- 0 to relation_num-1){
        sharedVar.declareArray("r" + i, dim) 
        sharedVar.setArray("r" + i, relation.getOrElse(i, null) )
      }
    
      for(i <- 0 to relation_num-1){
        for(j <- 0 to relation_num-1){
          sharedVar.declareArray("A"+i+j, dim) 
          sharedVar.setArray("A"+i+j, A(i).getOrElse(j, null) )
        }
      }
      
      for(i <- 0 to entity_num-1){
        sharedVar.declareArray("e" + i, dim)
        sharedVar.setArray("e" + i, entity.getOrElse(i, null))
      }  
      
      sharedVar.set("dim", dim)
      sharedVar.set("learningRate", learningRate)
      sharedVar.set("margin", margin )
      sharedVar.set("res",0.0)    
    })
   end = ((new Date).getTime).toDouble
   println("time for declaring shared variables = %5.8f;".format((end-strt)/1000))
   // Set stream processing functions
   paramRdd.setProcessFunction(process)
   // Create stream processing context
   val spc = new SplashConf().set("data.per.iteration", dataperiteration).set("auto.thread",false).set("max.thread.num", numPartitions)
   for( i <- 0 until NumIteration ){
      //set loss = 0
      strt = ((new Date).getTime).toDouble
      val temp = paramRdd.getSharedVariable().get("res")
      paramRdd.run(spc)
      LOG.info("iteration: "+i)
      // get the loss
      val loss = paramRdd.getSharedVariable().get("res")-temp
      //Print: running time, average loss and the thread number
      println("Time = %5.3f; Loss = %5.8f; Thread Number = %d".format(paramRdd.getTotalTimeEllapsed, loss, paramRdd.getLastIterationThreadNumber))
      end = ((new Date).getTime).toDouble
      println("time for loss iteration = %5.8f;".format((end-strt)/1000))
   }
   // display shared variables
   val paramVar = paramRdd.getSharedVariable();
   val rel_map = mutable.Map.empty[Int,String]
   val ent_map = mutable.Map.empty[Int,String]
   strt = ((new Date).getTime).toDouble
   for(i <- 0 to relation_num-1){
      rel_map(i) = paramVar.getArray("r"+i).mkString(" ")
   }
   end = ((new Date).getTime).toDouble
   println("time for retrieving relations = %5.8f;".format((end-strt)/1000))
   strt = ((new Date).getTime).toDouble
   for(i <- 0 to entity_num-1){
      ent_map(i) = paramVar.getArray("e"+i).mkString(" ")
   }
   end = ((new Date).getTime).toDouble
   println("time for retrieving entities = %5.8f;".format((end-strt)/1000))
   strt = ((new Date).getTime).toDouble
   sc.parallelize(rel_map.toSeq).map(f => f._1+","+f._2).saveAsTextFile(inputPath+"/rel")
   sc.parallelize(ent_map.toSeq).map(f => f._1+","+f._2).saveAsTextFile(inputPath+"/ent")
   end = ((new Date).getTime).toDouble
   println("time for saving files = %5.8f;".format((end-strt)/1000))
 } // main function
  
  
   // data processing function 
  val process = (elem: String, weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet) => {
    
    val ids = elem.split("\\s+")
    val e1 = ids(0).toInt
    val rel = ids(2).toInt
    val e2 = ids(1).toInt
    val e1c = ids(3).toInt
    val relc = ids(5).toInt
    val e2c = ids(4).toInt
    val relation_num = 250000
    // LOG.info("data:"+e1+" "+rel+" "+e2+" "+e1c+" "+relc+" "+e2c+" ")
    val rate = sharedVar.get("learningRate")
    val dim = sharedVar.get("dim").toInt
    val margin = sharedVar.get("margin")
    val stepsize = rate*2*weight
    
    // get weight vectors
    var w_e1 = sharedVar.getArray("e" + e1)    
    var w_e2 = sharedVar.getArray("e" + e2)    
    var w_r = sharedVar.getArray("r" + rel)
    
    var w_Mr = new Array[Array[Double]](relation_num)
    for (j<- 0 to relation_num){
     w_Mr(j) = sharedVar.getArray("A" + rel)
    }
    
    var w_e1c = sharedVar.getArray("e" + e1c)
    var w_e2c = sharedVar.getArray("e" + e2c)
    var w_rc = sharedVar.getArray("r" + relc)
    
    val X1 = get_residual(w_e1,w_e2,w_r,w_Mr)
    val X2 = get_residual(w_e1c,w_e2c,w_rc,w_Mr)
    // r = rc 
    var delta_e1 = new Array[Double](dim.toInt)
    var delta_e2 = new Array[Double](dim.toInt)
    var delta_rel = new Array[Double](dim.toInt)
    var delta_4 = new Array[Double](dim.toInt)
    var temp = new Array[Double](dim.toInt)
    
    var sum1 = norm(X1)
    var sum2 = norm(X2)
    var sign = 1.0
    if(sum1+margin  > sum2 ){
      sharedVar.add("res", weight*(sum1+margin-sum2))
     
      var dot = 0.0
      for (i <- 0 to dim.toInt){
         for (j <- 0 to dim.toInt){
          //dot1 += (w_hpr(i)*w_e1(i))
         }
      }
      
      var dot2 = 0.0
      for (i <- 0 to dim.toInt){
         dot2 += (w_hpr(i)*w_e2(i))
      }
      for(i <- 0 to dim-1){
        
        delta_e1(i) = rate*weight*X1(i)
        delta_e2(i) = -rate*weight*X2(i)
        delta_rel(i) = -rate*sign*dot1
        delta_4(i) =  rate*sign*dot2
      }
      sharedVar.addArray("e" +e1, delta_1)
      sharedVar.addArray("r" + rel, delta_1)
      sharedVar.addArray("e" + e2, delta_2)
      sharedVar.addArray("A" + rel, delta_3)
      sharedVar.addArray("A" + rel, delta_4)
      
      dot1 = 0.0
      for (i <- 0 to dim.toInt){
         dot1 += (w_hprc(i)*w_e1c(i))
      }
      dot2 = 0.0
      for (i <- 0 to dim.toInt){
         dot2 += (w_hprc(i)*w_e2c(i))
      }
      for(i <- 0 to dim -1){
        delta_1(i) = -rate*weight*sign
        delta_2(i) =  rate*weight*sign
        delta_3(i) =  rate*sign*dot1
        delta_4(i) = -rate*sign*dot2
      }
      sharedVar.addArray("e" +e1c, delta_2)
      sharedVar.addArray("r" + relc, delta_2)
      sharedVar.addArray("e" + e2c, delta_1)
      sharedVar.addArray("A" + rel, delta_3)
      sharedVar.addArray("A" + rel, delta_4)
    // Normalization to be done here if required
    
    }
  }
  * 
  */
}
