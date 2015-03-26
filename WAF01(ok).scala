package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.math._
import java.io.PrintWriter
import scala.collection.mutable
//
import org.apache.spark.SparkContext._


object SimpleApp {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    // ***************Now WAF*********
    val textFile = sc.textFile("/Users/apple/Documents/text0.txt").cache()
    //fi fj
    val termFre = textFile.flatMap(line => line.split("/")) .map(word => (word, 1)).reduceByKey(_ + _)
    val words  = termFre.count().toInt 
    val termFreToArray = termFre.collect()
	  val tMapId = termMapId(termFreToArray)
	  val textId = getTextID(textFile,tMapId) 
	  textFile.unpersist()
	  val fijAndD =textId.flatMap{line => 
		val len = line.length
		val arr = ArrayBuffer[((Int,Int),(Int,Int))]()	
		for(i <- 0 until len-1)
        	for(j <- i+1 until len) 
                arr += (((line(i),line(j)),(1,j-i)))
		             arr
		                	}.reduceByKey((a,b) =>tupleAdd(a,b))

	 val  waf= fijAndD.map(i => (i._1,1.0*i._2._1*i._2._1*i._2._1*i._2._1/(termFreToArray(i._1._1)._2*termFreToArray(i._1._2)._2*i._2._2*i._2._2))).filter(i => i._2 > 0.0001)
   val  wafLine = waf.map(i => (i._1._1,(i._1._2,i._2)))//.groupByKey().map(i => (i._1,i._2.toSeq))
   val  wafCol = waf.map(i => (i._1._2,(i._1._1,i._2)))//.groupByKey().map(i => (i._1,i._2.toSeq))
   val  wafInOut = wafCol.cogroup(wafLine).cache  
   //wafInOut.saveAsTextFile("WafInOut")
   textId.unpersist()

   val withoutWafV = wafInOut.map(i => (i._1,(i._2._1.map(j => j._1),i._2._2.map(k => k._1))))
   val simCouples = withoutWafV.map(i => (i._1,i._2._1++i._2._2)).flatMap(i =>i._2.map(j => (j,i._1))).groupByKey().map(i => i._2.toSeq).filter(_.length>1).flatMap{i => 
        val newSeq = mutable.Set.empty[(Int,Int)]
        val leng = i.length
        for(x <- 0 until leng-1 )
          for( y <- x+1 until leng)
            if (i(x)>i(y)) newSeq += ((i(y),i(x))) else newSeq += ((i(x),i(y)))
        newSeq 
        }.distinct
    //val wafInOutSortedByKey = wafInOut.sortByKey()

    val wafInOutSortedList = wafInOut.map(i => (i._1,sortKv(i._2._1.toList),sortKv(i._2._2.toList))).collect
    //定义一个数组Array[(List[(Int, Double)], List[(Int, Double)])]
    val wafInOutSorted = new Array[(List[(Int, Double)], List[(Int, Double)])](words)
    var j =0
    for (i <- wafInOutSortedList)
    {  j= i._1
      wafInOutSorted(j) = (i._2,i._3)
    }
    val wafVecBr = sc.broadcast(wafInOutSorted)
    
    val Avalue = simCouples.map(i => ((i._1,i._2),getA(i._1,i._2,wafVecBr))).filter(_._2>0).flatMap(i => Array((i._1._1,(i._2,i._1._2)),(i._1._2,(i._2,i._1._1))))
    val AvalueForSearch = Avalue.groupByKey().map(i => (i._1,sortKv2(i._2.toList)))
    
    val haveALook = AvalueForSearch.map(i => (termFreToArray(i._1)._1,i._2.map(j => (j._1,termFreToArray(j._2)._1 ))))
    haveALook.saveAsTextFile("AvalueReal")

    /*
    Array[(Int, (Iterable[Iterable[(Int, Int)]], Iterable[Iterable[(Int, Int)]]))] = Array((4,(CompactBuffer(CompactBuffer((1,250), (7,20), (0,1000), (8,111), (6,31))),CompactBuffer(CompactBuffer((5,55), (2,125), (3,1000))))),
	*/

  }
  def tupleAdd(a:(Int,Int),b:(Int,Int))= {
  	(a._1+b._1,a._2+b._2)
  }

  def termMapId(termAndFre:Array[(String, Int)])= {
  	val termMapId = mutable.Map.empty[String,Int]
  	var j = 0
  	for (i <- termAndFre){
    	termMapId += (i._1 -> j)
    	j +=1 
  	}
  	termMapId
  }
  // 每一句作为一个Array存储
  def getTextID(textFile:org.apache.spark.rdd.RDD[String],tMapId: scala.collection.mutable.Map[String,Int])=
  textFile.flatMap(line => line.split("\n")).map(  line =>line.split("/").map(word =>tMapId(word)) )
  // 整个文章展平表示为ID

  def sortKv(kv: List[(Int, Double)]):List[(Int, Double)]= {
    if (kv.isEmpty) kv
    else
     sortKv(kv.filter(x => x._1 < kv.head._1)):::kv.head::sortKv(kv.filter(x => x._1 > kv.head._1))
     }

   def sortKv2(kv: List[(Double,Int)]):List[(Double,Int)]= {
    if (kv.isEmpty) kv
    else
    sortKv2(kv.filter(x => x._1 > kv.head._1)):::kv.head::sortKv2(kv.filter(x => x._1 < kv.head._1))
  }


   def getA(i: Int,j: Int,wafVecBr:org.apache.spark.broadcast.Broadcast[Array[(List[(Int, Double)], List[(Int, Double)])]]) = {
    //wafVecBr.value(i) wafVecBr.value(j)
    def joinValue(la:List[(Int, Double)],lb:List[(Int, Double)]) : Double ={
      var k = 0
      var x:Double = 0
      def join(l1:List[(Int, Double)],l2:List[(Int, Double)]) : Double= {
         if (l1.isEmpty ) {
          k += l2.length
          0
        }
        else if(l2.isEmpty){
          k += l1.length
          0
        }
        else if (l1.head._1 < l2.head._1)  {
          k+=1
          join(l1.tail,l2)
        }
        else if (l1.head._1 > l2.head._1 ) {
          k+=1
          join(l1,l2.tail)
        }
        else if(l1.head._2 < l2.head._2)  {
          x += l1.head._2/l2.head._2
          k +=1
          join(l1.tail,l2.tail)
          }
          else{
            x += l2.head._2/l1.head._2
            k +=1
            join(l1.tail,l2.tail)
                }
              }
              join(la,lb)
              if(k==0)  0  else    x/k
            }
  
    if (wafVecBr.value(i)== null || wafVecBr.value(j) == null) 0
    else sqrt(joinValue(wafVecBr.value(i)._1,wafVecBr.value(j)._1)*joinValue(wafVecBr.value(i)._2,wafVecBr.value(j)._2))
          }

 
}
