import org.apache.spark.rdd._
import org.apache.spark.Partitioner
import archery._

class MyPartitioner(numParts:Int) extends Partitioner
{
  override def numPartitions:Int=numParts
  override def getPartition(key:Any):Int=
  {
    val id=key.toString.toInt % numParts                                //将c_id的第一位的字字母'D'去掉,将1000个快递员分成numParts个分区
    return id                                  
  }
  override def equals(other:Any):Boolean=other match
  {
    case mypartition:MyPartitioner =>
      mypartition.numPartitions==numPartitions
    case _ => false
  }
}

class Rtree_Partitioner(part_num:Int) extends Serializable
{
	var partid:Map[String,String]=Map()

	def partitioning(records:RDD[(Int,Record)], f_orders:Map[String,Order], e_orders:Map[String,Order_2], points_set:Map[String,Point_id])=
	{
		var check:List[String]=List()
		var entries:Array[(Point,String)]=Array()
		records.map{x=>x._2}.collect.foreach{x =>
			if(!check.contains(x.Cid))
            {
            	var entry=(Point(0.0F,0.0F)," ")
            	var place:String=" "
            	check=check:+x.Cid
            	if(x.Oid.charAt(0)=='F')
            		place=f_orders(x.Oid).tInd
            	else 
            		place=e_orders(x.Oid).tInd
            	var p=points_set(place)
            	entry=(Point(p.lng.toFloat,p.lat.toFloat),x.Cid)
            	entries=entries:+entry}}

	    var tree:RTree[String]=RTree()
	    entries.foreach{x=>tree=tree.insert(Entry(x._1,x._2))}
	    partid=tree.pretty.split("\n",0).map{x=>(x.split(" ")(1),x.split(" ")(0))}.toMap
	}

	def get_partitions(records:RDD[(Int,Record)], f_orders:Map[String,Order], e_orders:Map[String,Order_2], points_set:Map[String,Point_id]):RDD[Array[(Int,Record)]]=
	{
		var plan_origin=records.map(x=>(partid(x._2.Cid),x)).partitionBy(new MyPartitioner(part_num))

		//var final_best:(Double,Map[String,Array[String]],Array[Record])=(0.0,Map(),Array(Record("","")))
		var plan_partition=plan_origin.mapPartitions{x=>
			var sub_plan:Array[Array[(Int,Record)]]=Array()
			var sub_records:Array[(Int,Record)]=Array()
			while(x.hasNext)
			{
				var tmp=x.next()
				sub_records=sub_records:+tmp._2
			}
			sub_plan=sub_plan:+sub_records
			sub_plan.iterator}
		plan_partition.foreach{x => println("partition size: "+x.length)}
		return plan_partition
	}
}
