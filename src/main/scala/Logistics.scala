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
import archery._

object Logistics extends java.io.Serializable
{

  def main(args: Array[String])
  {

	val conf=new SparkConf().setAppName("Logistics")
	conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true")
	conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	conf.set("spark.kryo.registrationRequired", "false")
	conf.registerKryoClasses(Array(classOf[Tabu], classOf[Rtree_Partitioner],classOf[archery.Point], classOf[POINT], classOf[Route], classOf[Problem], classOf[DummyHeuristics], classOf[LocalSearch], classOf[IteratedLocalSearch],
	classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
	conf.set("spark.storage.memoryFraction","0.35")
	val sc=new SparkContext(conf)

    var Max_iter:Int = 0
    var Tabu_list_length=args(1).toInt
    var part_size=args(2).toInt
	val div_ratio=args(3).toDouble
	val thread_num=args(4).toInt


	var problem = new Problem()
	val data = problem.initial()
	println("initialed")


	var heuristics = new DummyHeuristics(problem)
	//val initial_sol = heuristics.get_solution(problem.customers)

	//var tabu = new Tabu(problem,30, 10, 10, 50,heuristics)
	//var get_solution = tabu.excute(initial_sol)

	var localsearch = new LocalSearch(problem)	
	/*var iteratedlocalsearch = new IteratedLocalSearch(problem, localsearch, initial_sol)
	var guidedlocalsearch = new GuidedLocalSearch(problem, iteratedlocalsearch, 0.5)*/

	//initial_sol.foreach{r => println(r.pretty())}
	//var get_solution = guidedlocalsearch.excute()

	println("beginning partitionning")
    var partitioner = new Rtree_Partitioner(part_size)
	partitioner.partitioning(sc.parallelize(data))
	var customer_par = partitioner.get_partitions(sc.parallelize(data))
	var total_solution = sc.parallelize(customer_par.map{x=> heuristics.get_solution(x)}.reduce{(x,y) => concat(x,y)}).persist(StorageLevel.MEMORY_AND_DISK_SER)

	var i = 0
	val start = System.currentTimeMillis()

	while(i < partitioner.level)
	{
		println(i)
		var par = partitioner.get_partitions(total_solution,i).persist(StorageLevel.MEMORY_AND_DISK_SER)

		var par_tmp = par.collect()
		val par_num = par_tmp.length
		while(par_tmp.length < thread_num)
		{
			par_tmp = par_tmp :+ par_tmp(par_tmp.length - par_num)
		}
		par = sc.parallelize(par_tmp).persist()

        var subtotal=par.map{x=>
			//var iteratedlocalsearch = new IteratedLocalSearch(problem, localsearch, x._2)
			//var guidedlocalsearch = new GuidedLocalSearch(problem, iteratedlocalsearch, 0.5)
			var tabu = new Tabu(problem, 2, 30, 10, 50, start, heuristics, 512)
			var plan_return= tabu.excute(x._2)

			(plan_return._1,plan_return._2,x._1)
		}.persist(StorageLevel.MEMORY_AND_DISK_SER)

		var best_subtotal = subtotal.map{x=>(x._3,x)}.reduceByKey{(par1,par2)=>
			if (par1._2 < par2._2)
				par1
			else
				par2
		}.map{x=>x._2}.persist(StorageLevel.MEMORY_AND_DISK_SER)

		i = i+1

		total_solution = sc.parallelize(best_subtotal.map{x => x._1}.reduce{(x,y)=> concat(x,y)}).persist()
		if ( i >= partitioner.level)
		{
			i = 0	
			var answers=best_subtotal.collect
			var count = 0
			answers(0)._1.foreach{r => println(r.pretty(problem)); count = count + r.customers.length}
			println(answers(0)._2,count)
		}
		par.unpersist()
		subtotal.unpersist()
		best_subtotal.unpersist()
	}
  }

}
