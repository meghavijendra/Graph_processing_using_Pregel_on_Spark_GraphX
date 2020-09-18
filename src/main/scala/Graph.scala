import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object GraphComponents {
  def main ( args: Array[String] ) {
        val config = new SparkConf().setAppName("Graph")
        val sc = new SparkContext(config)

        //Load Data and split it into Array
        var contentsRDD  = sc.textFile(args(0)).map(e => e.split(","))
        //Array(3,2,1) => Array(Array(Edge(3,2,3),Edge(3,1,3)),Array(Edge(2,1,2),Edge(2,4,2))) => Array(Edge,Edge,Edge,Edge)
        var edgeRdd: RDD[Edge[Long]]  = contentsRDD.map(e => e.slice(1,e.size).map(q => Edge(e(0).toLong,q.toLong,e(0).toLong))).flatMap(_.toList)
        //Creating Graph
        var graph = Graph.fromEdges(edgeRdd, "defaultProperty").mapVertices((id, _) => id)

        //Calling the GraphX Pregel API to find the connected components.
        var out = graph.pregel(Long.MaxValue, 5) ( (id, oldGroup, newGroup) => math.min(oldGroup, newGroup),
        triplet => {
                if (triplet.attr < triplet.dstAttr)
                {
                        Iterator((triplet.dstId, triplet.attr))
                } else if (triplet.srcAttr <  triplet.attr)
                {
                        Iterator((triplet.dstId, triplet.srcAttr))
                } else
                {
                        Iterator.empty
                }
        }, (a, b) => math.min(a, b) // finiding the minimum key and merge message
        )

        //Graph created by pregel get vertex and map the ID
        val result = out.vertices.map(vertex1 => (vertex1._2, 1)).reduceByKey(_ + _).sortByKey().collect().map(k => k._1.toString+' '+k._2.toString).foreach(println)
 }
}
