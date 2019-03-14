import org.apache.spark.rdd._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import archery._

class My_Partitioner(numParts:Int) extends Partitioner
{
  override def numPartitions:Int=numParts
  override def getPartition(key:Any):Int=
  {
    val id=key.toString.toInt                               //将c_id的第一位的字字母'D'去掉,将1000个快递员分成numParts个分区
    return id                                  
  }
  override def equals(other:Any):Boolean=other match
  {
    case mypartition:My_Partitioner =>
      mypartition.numPartitions==numPartitions
    case _ => false
  }
}

class Rtree_Partitioner(Part_Size:Int) extends Serializable
{
	//var partid:Array[(Int,Int)]=Array()
	var level:Int = 0
	var part_num:Int = 0
	var part_size:Int = Part_Size
	var leaf_map_order:Array[(Int,Array[Int])]=Array()
	var leaf_count:Array[Int]=Array()

	def partitioning(records:RDD[POINT])=
	{
		/*var entries:Array[(Point,Int)]=Array()
		records.collect.foreach{r =>
			var entry=(Point(r.x.toFloat,r.y.toFloat),r.number)
			entries=entries:+entry}*/

		var entries=records.map{r => (Point(r.x.toFloat,r.y.toFloat),r.number)}.collect()
		var tree:RTree[Int]=RTree()
		entries.foreach{x=>tree=tree.insert(Entry(x._1,x._2))}
		//partid=tree.pretty.split("\n",0).map{x=>(x.split(" ")(1),x.split(" ")(0).toInt)}.toMap
		var leaf_map_order_tmp=tree.pretty.split("\n",0).map{x=>
			var ids = x.split(" ")
			var id_array:Array[Int]=Array()
			val len = ids.length
			level = len-1
			var i =0
			for (i <-(0 to len-2).reverse)
				id_array = id_array :+ ids(i).toInt
			(ids(len-1).toInt,id_array)
		}

		var part_id_map:Map[Int,Int]=Map()
		var i =0;var j =0
		for (i<- 0 to level-1)
		{
			var visit:Array[Int] = Array()
			for (j<- 0 to leaf_map_order_tmp.length-1)
			{
				val level_id = leaf_map_order_tmp.map{x=>x._2(i)}.distinct
				val leaf_id_sort = level_id.sortWith{(a,b)=>a<b}
				var z = 0
				for(z<- 0 to leaf_id_sort.length-1)
					part_id_map += (leaf_id_sort(z) -> z)
			}
		}

		leaf_map_order = leaf_map_order_tmp.map{x=>(x._1,x._2.map{y => part_id_map(y)})}.toArray
		//leaf_map_order.toArray.foreach{x=> print(x._1+" ");x._2.foreach{y=>print(y+" ")};println()}
		//partid = leaf_map_order.map{x=>(x._1,x._2(0))}

		for (i <- 0 to level-1)
			leaf_count=leaf_count:+ leaf_map_order.map{x=>x._2(i)}.distinct.length
		part_num=leaf_count(0)
		//partid.toArray.sortWith((a,b) => a._2<b._2).foreach{x => print(x._1 +" "+x._2);println("")}
		leaf_count.foreach{x=>print(x + " ")};println("")
	}

	def get_partitions(records:RDD[POINT]):RDD[Array[POINT]]=
	{
		var plan_origin=records.map{x=>
			                   var i = 0
							   var find = false
							   var partid = (-1)
							   while ( i <= leaf_map_order.length-1 && find == false)
							   {
								   if (leaf_map_order(i)._1 == x.number)
								   {
									   find = true
									   partid = (leaf_map_order(i)._2)(0)
								   }
								   i = i+1
							   }
				               (partid,x)}
			                   .partitionBy(new My_Partitioner(part_num))

		//var final_best:(Double,Map[String,Array[String]],Array[Record])=(0.0,Map(),Array(Record("","")))
		var plan_partition=plan_origin.mapPartitions{x=>
			var sub_plan:Array[Array[(POINT)]]=Array()
			var sub_records:Array[(POINT)]=Array()
			while(x.hasNext)
			{
				var tmp=x.next()
				sub_records=sub_records:+tmp._2
			}
			sub_plan=sub_plan:+sub_records
			sub_plan.iterator}

		return plan_partition
	}

	def get_partitions(routes:RDD[Route],iter:Int):RDD[(Int,Array[Route])]=
	{
		var plan_origin=routes.map{x=>
			                  var i = 0
							  var find = false
							  var partid = (-1)
							  while ( i <= leaf_map_order.length-1 && find == false)
							  {
								  if (leaf_map_order(i)._1 == x.customers(0).number)
								  {
									  find = true
									  partid = (leaf_map_order(i)._2)(iter)
								  }
								  i = i+1
							  }
							  (partid,x)}
			                  .partitionBy(new My_Partitioner(leaf_count(iter)))

		//var final_best:(Double,Map[String,Array[String]],Array[Record])=(0.0,Map(),Array(Record("","")))
		var plan_partition=plan_origin.mapPartitionsWithIndex{(idx,x)=>
			var sub_plan:Array[(Int,Array[Route])]=Array()
			var sub_routes:Array[Route]=Array()
			while(x.hasNext)
			{
				var tmp=x.next()
				sub_routes=sub_routes:+tmp._2
			}
			sub_plan=sub_plan:+(idx,sub_routes)
			sub_plan.iterator}

		//plan_partition.foreach{x => println("partition size: "+x._1+" "+x._2.length)}
		return plan_partition
	}


}
