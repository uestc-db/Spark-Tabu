import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ 
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark
import java.io._
import collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import Array._
import org.apache.spark.Partitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
import scala.io.Source

object Logistics extends java.io.Serializable
{

  def main(args: Array[String])
  {

	val conf=new SparkConf().setAppName("Logistics")
	val sc=new SparkContext(conf)
	val util_define=new Util()
    val combine=new Combine()
	 val vector_combiner = new Solution_Vector()

    val start = System.nanoTime()
    var Max_iter:Int = 0
	val total_courier:Int = 1000
    var Tabu_list_length=args(1).toInt
    var part_num=args(2).toInt
    var data=util_define.read_data()
	var points_set=data._1.union(data._2.union(data._3)).toMap            //用于dist函数
	var site_points=data._1.toMap                                   //用于get_flist和get_elist函数
    var shop_points=data._3.toMap                                    //用于get_elist函数
    val f_orders=util_define.readOrder("hdfs://master:9000/user/liuyi/logistics/data/4.csv").toMap
    val e_orders=util_define.readOrder2("hdfs://master:9000/user/liuyi/logistics/data/5.csv").toMap
    val c_ids_all=util_define.readCourier("hdfs://master:9000/user/liuyi/logistics/data/6.csv")
    val records_all:Array[(Int,Record)]=util_define.readRecord("hdfs://master:9000/user/liuyi/logistics/data/e-com.csv")
    var plans:Array[Array[Record]]=Array()
	var Courier_counter:Map[String,Int]=Map()
	var history_tabu_list:Array[String]=Array()
    //val writer=new PrintWriter(new File(args(5)))
    //val vector_writer = new PrintWriter(new File(args(6)))
	var vector:Array[(Int,Vector)]=Array()
    var final_best:(Double,Map[String,Array[String]],Array[Record])=(0.0,Map(),Array(Record("","")))

    
    var c_ids:Array[Courier]=Array()
    var records:Array[(Int,Record)]=Array()
    var index=0
    for(index <-0 to total_courier-1)
      c_ids=c_ids:+c_ids_all(index)
    records_all.foreach{x=> if (c_ids.contains(Courier(x._2.Cid))) records=records:+x}
    c_ids.foreach{x=> Courier_counter+=(x.Cid -> 0)}

    val rtree_patitioner = new Rtree_Partitioner(part_num)
	rtree_patitioner.partitioning(sc.parallelize(records), f_orders, e_orders, points_set)
    var plan_partition = rtree_patitioner.get_partitions(sc.parallelize(records), f_orders, e_orders, points_set)
	plan_partition.cache()

    
    var combine_iter= Math.log(part_num)/Math.log(2)
	var repartition_count:Int=0
	var continue_flag:Int =0
    while(combine_iter >= 0)
    {
		//Max_iter = total_courier/part_num/2-(combine_iter+1).toInt*3
		Max_iter=total_courier*(combine_iter+1).toInt/part_num/20
		val tabu_executor = new Tabu(Max_iter, Tabu_list_length,args(3).toDouble,start)
        var subtotal1=plan_partition.map{x=>
			var sub_cids:Array[Courier]=Array()
			/*println("send data:")
			x.foreach{y=> print(y._2 +" ")}
			println("")*/
			x.foreach{y=>if(!sub_cids.contains(y._2.Cid)) sub_cids=sub_cids:+Courier(y._2.Cid)}
		
			var plan_return=tabu_executor.tabu(f_orders,e_orders,sub_cids,x,points_set,site_points,shop_points,history_tabu_list,Courier_counter,"type_4")
			plan_return
		}

        subtotal1.cache()

        if(combine_iter == 0)
        {
			val answers = subtotal1.collect.sortWith((a,b) => (a._1 < b._1))
			if(repartition_count < 500 )
			{
				plan_partition = rtree_patitioner.get_partitions(sc.parallelize(answers(0)._3), f_orders, e_orders, points_set)
				plan_partition.cache()
				continue_flag=1
				repartition_count = repartition_count+1
			}
			final_best = (answers(0)._1, answers(0)._2, answers(0)._3.map{x=>x._2})
			vector = vector.union(subtotal1.map{x => x._4}.flatMap(x=>x.toList).sortByKey(true).collect)

			//vectors.foreach{x => vector_writer.write(x._1+" "+x._2.total_cost+" "+x._2.max_customer_num+" "+x._2.median_customer_num+" "+x._2.min_customer_num+" "+x._2.total_path+" "+x._2.ave_length+" "+x._2.ave_arc_length+" "+x._2.ave_cost+"\r\n")}
		}
      
		if (combine_iter != 0)
		{

			history_tabu_list = subtotal1.map{x => x._5}.flatMap(x=>x.toList).distinct.collect
			Courier_counter = subtotal1.map{x => x._6.toArray}.flatMap{x=>x.toList}.reduceByKey{(x,y)=>x+y}.collect.toMap

			var idx= (-1)
			val par_rec=subtotal1.map{x=>(x._1, (combine.rec_bound(x._3,f_orders,e_orders,points_set), x._3, x._4))}                        //计算每个分区的MBR
			par_rec.cache()
			val group_rec=par_rec.partitionBy(new HashPartitioner(1))
								 .map{x=>idx+=1;(idx, x._1, x._2._1, x._2._2, x._2._3)}
                                 .groupBy{x => x._3}
			group_rec.cache()

			idx = (-1)
			val id_rec_group= group_rec.partitionBy(new HashPartitioner(1))
                                       .map{x => idx+=1; (idx, x._1, x._2.toArray)}
			id_rec_group.cache()

			id_rec_group.foreach{x=>print(x._1+" "+x._2+" ");x._3.foreach{y => print(y)};println("")}

			val solution_vector = id_rec_group.map{x => x._3.map{y => y._5}}
											  .reduce{(sol1,sol2)=>
												  var i = 0
												  var combined_vector:Array[Array[(Int,Vector)]]=Array()
												  for (i <- 0 to sol1.length-1)
												  combined_vector = combined_vector :+ vector_combiner.combine_solution_vector(sol1(i),sol2(i))
												  combined_vector}
											  .flatMap{x=>x.toList}.sortWith((a,b) => (a._1 < b._1))
			//solution_vector.sortWith((a,b) => (a._1 < b._1))
			//               .foreach{x => vector_writer.write(x._1+" "+x._2.total_cost+" "+x._2.max_customer_num+" "+x._2.median_customer_num+" "+x._2.min_customer_num+" "+x._2.total_path+" "+x._2.ave_length+" "+x._2.ave_arc_length+" "+x._2.ave_cost+"\r\n")}
			vector = vector.union(solution_vector)

			val id_rec = id_rec_group.map{x=> (x._1,x._2)}.collect()
			var recid_key=combine.get_pairs(id_rec)
			val combined_par=id_rec_group.map{x => (recid_key(x._1), x._3)}
										 .reduceByKey{(part1, part2)=>
											 val sorted1 = part1.sortWith((a,b) => (a._2 < b._2))
											 val sorted2 = part2.sortWith((a,b) => (a._2 < b._2))
											 var i =0;
											 var combined_data:Array[(Int,Double,(Double,Double,Double,Double),Array[(Int,Record)],Array[(Int,Vector)])]=Array()
											 for (i <- 0 to sorted1.length-1)
												 combined_data = combined_data :+ (0,0.0,(0.0,0.0,0.0,0.0),concat(sorted1(i)._4,sorted2(i)._4),Array((0,Vector(0.0,0,0,0,0,0,0,0.0))))
											 combined_data}
										 .values.flatMap{x => x.toList}.map{x=> x._4}
			val combined=combined_par.collect()
			plan_partition=sc.parallelize(combined.union(combined))
		}
		if(continue_flag == 1)
		{
			combine_iter= Math.log(part_num)/Math.log(2)
			continue_flag = 0
		}
		else
			combine_iter = combine_iter-1
    }

    
    //writer.write("best plan:"+final_best._1+"\r\n")
    //final_best._3.foreach{x=>writer.write(x.Cid+" "+x.Oid+"\r\n")}
	sc.parallelize(vector).coalesce(1,true).saveAsTextFile(args(5))
	sc.parallelize(final_best._3).coalesce(1,true).saveAsTextFile(args(4))
    //vector_writer.close()
    //writer.close()
  }
}
