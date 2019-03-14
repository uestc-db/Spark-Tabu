import scala.collection.mutable.ArrayBuffer
import Array._
import scala.io.Source
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.FSDataInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import scala.util.Random


class POINT(input_number:Int,input_x:Double,input_y:Double,input_demand:Int,input_ready_time:Double,input_due_time:Double,input_service_time:Double) extends Serializable
{
	val number = input_number
	val x = input_x
	val y = input_y
	val demand = input_demand
	val ready_time = input_ready_time
	val due_time = input_due_time
	val service_time = input_service_time
	var idx = -1

	def distance(target:POINT):Double=
	{
		return Math.sqrt(Math.pow(x-target.x,2)+Math.pow(y-target.y,2))
	}

	def set_idx(input_idx:Int)
	{
		idx = input_idx
	}
}

class Route(cus:Array[POINT]) extends Serializable
{
	var customers = cus

	/*def get_customers(cus:Array[POINT]):Array[POINT]=
	{
		if (cus.length == 0)
			return cus
		else
			return problem.cus_dep(cus(0)) +: cus :+  problem.cus_dep(cus(0))
	}*/
    def pretty(problem:Problem):String=
	{
		if (customers.length == 0)
			return " "
		var tmp_customers = problem.cus_dep(customers(0).number) +: customers :+  problem.cus_dep(customers(0).number)
		var sb = ""
		tmp_customers.foreach{p => sb = sb + p.number + " "}
		sb = sb + ":"
		tmp_customers.foreach{p => sb = sb + p.demand + " "}
		return sb
	}

	def set_idx()
	{
		customers.foreach{x => x.set_idx(customers.indexOf(x))}
	}

	/*def get_depot():POINT=
	{
		if (customers.length == 0)
			return new POINT(-1, 0.0, 0.0, 0, 0.0, 0.0, 0.0)
		return problem.cus_dep(customers(0).number)
	}*/

	def get_idx(cus_num:Int):Int=
	{
		if (customers.length == 0)
			return (-1)
		var i = 0
		for (i <- 0 to customers.length-1)
		{
			if (customers(i).number == cus_num)
				return i
		}
		return (-1)
	}

	def edges():Array[(POINT,POINT)]=
	{
		var i = 0
		var edges:Array[(POINT,POINT)]=Array()
		for (i<- 0 to customers.length-2)
			edges = edges :+ (customers(i) ,customers(i+1))
		return edges
	}

	def total_distance(problem:Problem):Double=
	{
		if (customers.length == 0)
			return 0.0
		var i = 0
		var dis = 0.0
		var tmp_customers = problem.cus_dep(customers(0).number) +: customers :+  problem.cus_dep(customers(0).number)
		for (i <- 0 to tmp_customers.length-2)
			dis = dis + tmp_customers(i).distance(tmp_customers(i+1))
		return dis
	}

	def total_cost(pen:Double, problem:Problem):Double=
	{
		if (customers.length == 0)
			return 0.0
		var time = 0.0
		var capacity = problem.vehicle_capacity
		var penalsum = 0.0
		var i = 0
		var tmp_customers = problem.cus_dep(customers(0).number) +: customers :+  problem.cus_dep(customers(0).number)
        for (i <- 0 to tmp_customers.length-2)
        {
            val start_service_time = Math.max(tmp_customers(i+1).ready_time, time + tmp_customers(i).distance(tmp_customers(i+1)))
            if (start_service_time >= tmp_customers(i+1).due_time)
            	penalsum = penalsum + (start_service_time - tmp_customers(i+1).due_time)
            time = start_service_time + tmp_customers(i+1).service_time
            capacity -= tmp_customers(i+1).demand
        }
        if (capacity <0)
        	penalsum = penalsum - capacity
        return time + penalsum * pen
	}

	def is_feasible(problem:Problem):Boolean=
	{
		if (customers.length == 0)
			return true
        var time = 0.0
        var capacity = problem.vehicle_capacity
        var is_feasible = true
        var i = 0
		var tmp_customers = problem.cus_dep(customers(0).number) +: customers :+  problem.cus_dep(customers(0).number)
        for (i <- 0 to tmp_customers.length-2)
        {
            val start_service_time = Math.max(tmp_customers(i+1).ready_time, time + tmp_customers(i).distance(tmp_customers(i+1)))
            if (start_service_time >= tmp_customers(i+1).due_time)
                is_feasible = false
            time = start_service_time + tmp_customers(i+1).service_time
            capacity -= tmp_customers(i+1).demand
        }
        if (time >= problem.depot_due_time || capacity < 0)
            is_feasible = false
        return is_feasible
    }

    def info(pen:Double, problem:Problem):Array[(Int, Double, Double,Double)]=                     //number, pre_dist, shortest_dis
    {
    	if (customers.length == 0)
    		return Array()
    	var tmp_customers = problem.cus_dep(customers(0).number) +: customers :+  problem.cus_dep(customers(0).number)
    	var cus_info:Array[(Int, Double, Double,Double)]=Array()
    	var i = 0
    	val ori_cost = total_cost(pen, problem)
    	for ( i <- 1 to customers.length-2)
    	{
    		var n = customers(i).number
    		var r = new Route(customers.filter{x=>x.number != n})
    		val new_cost = r.total_cost(pen, problem)
    		cus_info = cus_info :+ (tmp_customers(i).number, tmp_customers(i).distance(tmp_customers(i-1)), problem.nearest_distance(tmp_customers(i).number), ori_cost-new_cost)
			r = null
    	}
    	return cus_info
    }
}

class Problem() extends Serializable
{
	//var customers:Array[POINT] = Array()
	//var depot_set:Array[POINT]=Array()
	var cus_dep:Map[Int,POINT] = Map()
	var nearest_distance:Map[Int,Double] = Map()
	//var k_neighbors:Map[Int,Array[Int]]=Map()
	var vehicle_number = 0
    var vehicle_capacity = 0
    var depot_due_time = 0.0

    def initial():Array[POINT]=
    {
    	val read = read_data("hdfs://Paradise4Coder:9000/user/liuyi/logistics/data/Brussels1_data.txt")
    	val customers = read._1
    	val depot_set = read._2
		//customers.foreach{c=>println(c.number+" "+c.x+" "+c.y+" "+c.demand+" "+c.ready_time+" "+c.due_time+" "+c.service_time)}
    	vehicle_capacity = 50
    	depot_due_time = depot_set(0).due_time

    	customers.foreach{c=>
    		var dep_dis:Array[(POINT,Double)]=Array()
    		depot_set.foreach{d=>
    			dep_dis = dep_dis:+(d, c.distance(d))
    		}
    		val sort_dis = dep_dis.sortWith{(a,b) => (a._2 < b._2)}
    		cus_dep += (c.number-> sort_dis(0)._1)
    	}

		println("getting nearest neighbors")
    	nearest_distance = customers.map{c1=>
    		val dis_list = customers.map{c2 => (c2, c1.distance(c2))}
    		val sort_list = dis_list.sortWith{(a,b) => (a._2 < b._2)}
    		(c1.number, sort_list(0)._2)
    	}.toMap
		println("got nearest neighbors")
		return customers
    }

    def read_data(path:String):(Array[POINT], Array[POINT])=
  	{
	  	var rows = ArrayBuffer[Array[String]]()
	  	var line:String="begin"
	  	val hdfs = FileSystem.get(URI.create(path), new Configuration)
	  	var fp : FSDataInputStream = hdfs.open(new org.apache.hadoop.fs.Path(path))
	  	var isr : InputStreamReader = new InputStreamReader(fp)
	  	var bReader : BufferedReader = new BufferedReader(isr)

	  	while(line!=null)
	  	{
		  	line = bReader.readLine()
		  	if(!"".equals(line) && line!=null)
			  	rows += line.split(",").map(_.trim)
	  	}
	  	isr.close()
	  	bReader.close()
	  
	  	var i:Int =0
	  	var read_points:Array[POINT]=Array()
	  	for ( i <- 0 to rows.length-1)
	  		read_points = read_points :+ new POINT(rows(i)(0).toDouble.toInt, rows(i)(1).toDouble, rows(i)(2).toDouble, rows(i)(3).toDouble.toInt, rows(i)(4).toDouble, rows(i)(5).toDouble, rows(i)(6).toDouble)
	  	val read_customers = read_points.filter{x => x.demand != 0}
	  	val read_depots = read_points.filter{x => x.demand == 0}

	  	return (read_customers, read_depots)
  	}

  	def obj_func(routes:Array[Route], pen:Double):Double=
  	{
  		var total_cost : Double = 0
  		routes.foreach{x=>
  			total_cost = total_cost + x.total_cost(pen,this)
  		}
  		return total_cost
  	}

  	def is_feasible(routes:Array[Route]):Boolean=
	{
		var is_feasible = true
		routes.foreach{r => if (!r.is_feasible(this)) is_feasible = false}
		return is_feasible
	}

	def update_neighbors(customers:Array[POINT]):Map[Int,Array[Int]] = Map()
	{
		def my_shuffle(array: Array[Int]): Array[Int] = 
		{
			val rnd = new java.util.Random
 			for (n <- Iterator.range(array.length - 1, 0, -1)) 
 			{
				val k = rnd.nextInt(n + 1)
				val t = array(k); array(k) = array(n); array(n) = t
 			}
 			return array
		}

		var k_neighbors:Map[Int,Array[Int]]= Map()
		customers.foreach{c1=>
    		var c1_can:Array[Int]=Array()
    		customers.foreach{c2=>
    			c1_can = c1_can :+ c2.number
    		}
    		val sort_c1_can = my_shuffle(c1_can).slice(0,50)
			//val sort_c1_can = c1_can.sortWith{(a,b) => a._2 < b._2}.slice(0,50).map{x => x._1}
    		k_neighbors += (c1.number -> sort_c1_can)
    	}
    	//println("updated......")
		return k_neighbors
	}
}

class DummyHeuristics(pro:Problem) extends Serializable
{
	val problem = pro

	def get_solution(cus_list:Array[POINT]):Array[Route]=
	{

		def get_available_customers(customer_candidate:Array[POINT]):Array[POINT]=
		{
			return customer_candidate.sortWith{(a,b)=>a.due_time < b.due_time}
		}

		var solution:Array[Route] = Array()
		var c_item = new POINT(-1, 0.0, 0.0, 0, 0.0, 0.0, 0.0)
		var customers_list = cus_list
		while(get_available_customers(customers_list).length> 0)
		{
			var get_cus = get_available_customers(customers_list)
			var route =new  Route(Array())
			for (c_item <- get_cus)
			{
				route.customers = route.customers :+ c_item
				if (!route.is_feasible(problem))
					route.customers = route.customers.filter{x => x.number != c_item.number}
				else
					customers_list = customers_list.filter{x => x.number != c_item.number}
			}
			route.set_idx()
			solution = solution :+ route
		}
		return solution
	}

	def merge_solution(routes:Array[Route], pen:Double):Array[Route]=
	{
		var cur_solution = routes
		if (cur_solution.length == 0)
			return Array()
		var i = 0
		var shortest_len = cur_solution(0).customers.length
		var shosrtest_idx = 0
		for (i <- 0 to cur_solution.length - 1)
		{
			if (cur_solution(i).customers.length < shortest_len)
			{
				shortest_len = cur_solution(i).customers.length
				shosrtest_idx = i
			}
		}

		val merge_cus = cur_solution(shosrtest_idx).customers.clone()
		var item  = new POINT(-1, 0.0, 0.0, 0, 0.0, 0.0, 0.0)
		var solution_list:Array[(Array[Route],Double)]=Array()

		for (item <- merge_cus)
		{
			//val loc = heuristics.cus_location(cur_solution, item)
			solution_list = Array()
			var i = 0; var j = 0
			var loc = -1
			for (i <- 0 to cur_solution(shosrtest_idx).customers.length - 1)
			{
				if (cur_solution(shosrtest_idx).customers(i).number == item.number)
					loc = i

			}
			if (loc!=(-1))
			{
				for (i <- 0 to cur_solution.length-1)
				{
					if (i != shosrtest_idx)
					{
						for (j <- 0 to cur_solution(i).customers.length-1)
						{
							var c = operate(cur_solution(shosrtest_idx).customers, cur_solution(i).customers, loc, j, 1)
							var r1 = new Route(c._1)
							var r2 = new Route(c._2)
							var new_solution = cur_solution.toBuffer
							new_solution(shosrtest_idx)=r1
							new_solution(i)=r2
							val cur_cost:Double = problem.obj_func(new_solution.toArray, pen)
							solution_list = solution_list:+(new_solution.toArray, cur_cost)
							r1 = null
							r2 = null
								//println(inital_cost+" "+ cur_cost+ " "+Math.abs(inital_cost - cur_cost)/inital_cost)
						}
					}
				}
			}
			val sort_solution_list = solution_list.sortWith{(a,b) => a._2 < b._2}
			if (sort_solution_list.length != 0)
				cur_solution = sort_solution_list(0)._1
		}
		cur_solution = cur_solution.filter{x=> x.customers.length!=0}
		return cur_solution
	}

	def unimproved_list(routes:Array[Route],pen:Double):Array[Int]=
	{
		var candidate_list:Array[(Int,Double, Double,Double)]=Array()
		routes.foreach{x => candidate_list = concat(candidate_list,x.info(pen, problem))}
	    val return_list = candidate_list.sortWith{(a,b) => 
	    	                                       if ((a._2 - a._3) == (b._2 - b._3))
	    	                                   	       a._4 > b._4
	    	                                   	   else
	    	                                   	       (a._2 - a._3) > (b._2 - b._3)}.map{x=> x._1}
	    return return_list

	}

	def unimproved_cus(routes:Array[Route],pen:Double):Int=
	{
		var candidate_list:Array[(Int,Double, Double,Double)]=Array()
		routes.foreach{x => candidate_list = concat(candidate_list,x.info(pen, problem))}
	    val return_list = candidate_list.sortWith{(a,b) => (a._2 - a._3) > (b._2 - b._3)}
	    if (return_list.length == 0)
	    	return -1
	    return return_list(0)._1
	}


	def peice(a:Array[POINT], i:Int, j:Int):Array[POINT]=
	{
		if (i > j || i < 0 || j > a.length-1 || i > a.length-1 )
			return Array()
		var new_a:Array[POINT]=Array()
		var idx = 0
		for (idx <- i to j)
			new_a = new_a :+ a(idx)
		return new_a
	}

	def two_opt(a:Array[POINT], i:Int, j:Int):Array[POINT]=
	{
		var new_a:Array[POINT]=Array()
		if (i == 0)
			new_a = concat(peice(a,1,j) :+ a(0),peice(a, j+1,a.length-1))
		else
			new_a = concat(concat(peice(a, 0,i),peice(a, i+1,j).reverse), peice(a, j+1,a.length-1))
		return new_a
	}

	def insertion_intra(a:Array[POINT] ,i:Int, j:Int):Array[POINT]=
	{
		if (a.length == 0 || i < 0 || i >= a.length || j<0 || j > a.length)
			return a
		var new_a:Array[POINT]=Array()
		new_a = concat(peice(a, 0, i-1), peice(a, i+1, a.length-1))
		new_a = concat(peice(new_a, 0, j-1) :+ a(i), peice(new_a, j, new_a.length-1))
		return new_a

	}

	def cross(a:Array[POINT], b:Array[POINT], i:Int, j:Int):(Array[POINT],Array[POINT])=
	{
		var new_a:Array[POINT] = Array()
		var new_b:Array[POINT] = Array()
		new_a = concat(peice(a,0,i), peice(b,j+1,b.length-1))
		new_b = concat(peice(b,0,j), peice(a,i+1,a.length-1))
		return (new_a, new_b)
	}

	def	insertion(a:Array[POINT], b:Array[POINT], i:Int, j:Int):(Array[POINT],Array[POINT])=
	{
		if (a.length == 0 || i < 0 || i >= a.length)
			return (a,b)
		var new_a:Array[POINT] = Array()
		var new_b:Array[POINT] = Array()
		new_a = concat(peice(a,0,i-1),peice(a,i+1,a.length-1))
		new_b = concat(peice(b,0,j-1) :+ a(i),peice(b,j,b.length-1))
		return (new_a, new_b)
	}

	def swap(a:Array[POINT], b:Array[POINT], i:Int, j:Int):(Array[POINT],Array[POINT])=
	{
		if (i <0 || j <0 ||i >= a.length || j >= b.length)
			return (a,b)
		var new_a:Array[POINT] = Array()
		var new_b:Array[POINT] = Array()
		new_a = concat(peice(a,0,i-1) :+ b(j), peice(a,i+1,a.length-1))
		new_b = concat(peice(b,0,j-1) :+ a(i), peice(b,j+1,b.length-1))
		return (new_a, new_b)
	}

	def swap_pos(a:Array[POINT], b:Array[POINT], i:Int, j:Int):(Array[POINT],Array[POINT])=
	{
		if (i <0 || j <0 ||i >= a.length || j >= b.length || i >= b.length || j >= a.length)
			return (a,b)
		var new_a:Array[POINT] = concat(peice(a,0,i-1),peice(a,i+1,a.length-1))
		var new_b:Array[POINT] = concat(peice(b,0,j-1),peice(b,j+1,b.length-1))
		new_a = concat(peice(new_a,0,j-1) :+ b(j) , peice(new_a, j, new_a.length-1))
		new_b = concat(peice(new_b,0,i-1) :+ a(i),  peice(new_b, i, new_b.length-1))
		return (new_a, new_b)
	}

	def one_swap_two(a:Array[POINT], b:Array[POINT], i:Int, j:Int, k:Int):(Array[POINT],Array[POINT])=
	{
		if (i <0 || j <0 || k <0 ||i >= a.length || j >= b.length-1|| k >= b.length-1 || j>k)
			return (a,b)
		var new_a:Array[POINT] = Array()
		var new_b:Array[POINT] = Array()
		new_a = concat(peice(a,0,i-1):+b(j):+b(k),  peice(a,i+1,a.length-1))
		new_b = concat(concat(peice(b,0,j-1) :+ a(i), peice(b,j+1,k-1)), peice(b,k+1,b.length-1))
		return (new_a,new_b)
	}

	def operate(a:Array[POINT], b:Array[POINT], i:Int, j:Int,mode:Int):(Array[POINT],Array[POINT])=
	{
		if (mode == 0)
			return cross(a,b,i,j)
		else if (mode == 1)
			return insertion(a,b,i,j)
		else if (mode == 2)
			return swap(a,b,i,j)
		else if (mode == 3)
			return swap_pos(a,b,i,j)
		else
			return one_swap_two(a,b,i,j,j+1)
	}

	def cus_location(routes:Array[Route], cus_num:Int):(Int, Int)=                 //route_idx, cus_idx
	{
		var j = 0
		for (j <- 0 to routes.length-1)
		{
			val cus_ids = routes(j).customers.map{x=>x.number}
			if (cus_ids.contains(cus_num))
				return (j, routes(j).get_idx(cus_num))
		}
		return (-1, -1)
	}

}

class LocalSearch(pro:Problem) extends Serializable
{
	val problem = pro
	var heuristics = new DummyHeuristics(problem)

	def optimize (solution:Array[Route]):Array[Route]=
	{
		var new_solution = solution.toBuffer
		var i = 0
		var is_stuckd = false
		while(!is_stuckd)
		{
			is_stuckd = true
			for (i <- 0 to new_solution.length-1)
			{
				var route = new_solution(i)
				var best_route = new_solution(i)
				var k =0;var j = 0
				for( k <- 0 to route.customers.length-1)
				{
					for (j <- k+1 to route.customers.length-1)
					{
						val new_route = new Route(heuristics.two_opt(route.customers,k,j))
						var randnum=(new Random).nextInt(10)
						if ( new_route.total_cost(0,problem) < best_route.total_cost(0,problem) || randnum < 2 )
						{
							best_route = new_route
							if ( new_route.total_cost(0, problem) < best_route.total_cost(0, problem))
								is_stuckd = false
						}
					}
				}
				new_solution(i) = best_route
			}
		}
		return new_solution.toArray
	}
}

class IteratedLocalSearch(pro:Problem, local:LocalSearch, ini_sol:Array[Route]) extends Serializable
{
	var problem:Problem = pro
	val localsearch = local
	var initial_solution:Array[Route] = ini_sol
	var heuristics = new DummyHeuristics(problem)

	def perturbation(routes:Array[Route]):Array[Route]=
	{
		var best = routes.toBuffer
		var is_stuckd = false
		while(!is_stuckd)
		{
			is_stuckd = true
			var i = 0;var j =0; var k = 0; var l = 0; var m = 0
			for(i<- 0 to best.length-1)
			{
				for(j<- 0 to best.length-1)
				{
					if (i != j)
					{
						var best_i = best(i)
						var best_j = best(j)
						var randnum=(new Random).nextInt(3)
						for(k <- 0 to best(i).customers.length-1)
						{
							for (l <- 0 to best(j).customers.length-1)
							{
								var c = heuristics.operate(best(i).customers, best(j).customers, k, l,randnum)
								var r1 = new Route(c._1)
								var r2 = new Route(c._2)
								if (r1.is_feasible(problem) && r2.is_feasible(problem))
								{
									if (r1.total_cost(0, problem) + r2.total_cost(0,problem) < best_i.total_cost(0, problem) + best_j.total_cost(0, problem))
									{
										best_i = r1
										best_j = r2
										is_stuckd = false
									}
								}
							}
						}
						best(i)=best_i
						best(j)=best_j
					}
				}
			}
			best = best.filter{x=> x.customers.length!=0}
		}
		return best.toArray
	}

	def excute(routes:Array[Route], penalties:Array[Array[Double]], l:Double):Array[Route]=
	{
		def augmented_obj_func(routes:Array[Route]):Double=
		{

			var total_edges:Array[(POINT,POINT)]=Array()
		    routes.foreach{x => total_edges = concat(total_edges, x.edges())}
	 		var g = problem.obj_func(routes, 0)
	 		var penalty_sum = 0.0
	 		total_edges.foreach{x => penalty_sum = penalty_sum + penalties(x._1.number)(x._2.number)}
	 		return g+l*penalty_sum
		}

		var best = routes
		var cur_solution= best
		//println("Local search solution: Total cost: "+problem.obj_func(best))

		var is_stuckd = false
		while(!is_stuckd)
		{
			is_stuckd = true
			var new_solution = localsearch.optimize(cur_solution)
			new_solution = perturbation(new_solution)
			if(problem.is_feasible(new_solution) && augmented_obj_func(new_solution) < augmented_obj_func(best))
			{
				is_stuckd = false
				best = new_solution.filter{x=> x.customers.length != 0}
				var count = 0
				best.foreach{x=> count=count+x.customers.length}
				//println("Total cost: "+augmented_obj_func(best)+ " "+ count)
			}
		}
		return best
	}

}

/*class GuidedLocalSearch(pro:Problem, iterated:IteratedLocalSearch, l:Double) extends Serializable
{
	val problem = pro
	val iteratedlocalsearch = iterated
	var L = l
	var penalties =  ofDim[Double](problem.customers.length+1, problem.customers.length+1)

	def numeric_edges(routes:Array[Route]):Array[(POINT,POINT)]=
	{
		var total_edges:Array[(POINT,POINT)]=Array()
		routes.foreach{x => total_edges = concat(total_edges, x.edges())}
		return total_edges
	}


	 def update_penalties(routes:Array[Route])
	 {
	 	var util = ofDim[Double](problem.customers.length+1, problem.customers.length+1)

	 	numeric_edges(routes).foreach{e=>util(e._1.number)(e._2.number)=e._1.distance(e._2)/(1+penalties(e._1.number)(e._2.number))}
	 	val max_util_value = util.map{x=>x.max}.max
	 	var i = 0;var j = 0
	 	for (i <- 0 to problem.customers.length)
	 	{
	 		for ( j <- 0 to problem.customers.length)
	 		{
	 			if(util(i)(j) == max_util_value)
	 				penalties(i)(j) = penalties(i)(j)+1
	 		}
	 	}

	 }

	 def excute():Array[Route]=
	 {
	 	var cur_solution = iteratedlocalsearch.initial_solution
	 	var best:Array[Route] = cur_solution
	 	var iter = 0
	 	println("Guided search solution: Total cost: "+problem.obj_func(best, l))
	 	while(iter < 5)
	 	{
	 		var k =0
	 		var solution:Array[Array[Route]]=Array()
	 		while (k < 10)
	 		{
	 			val local_min = iteratedlocalsearch.excute(best,penalties,l)
	 			println("Local search solution: Total cost: "+problem.obj_func(local_min,l))
	 			update_penalties(local_min)
	 			solution = solution :+ local_min
	 			k = k + 1
	 		}

	 		val sort_solution=solution.sortWith{(a,b)=> (problem.obj_func(a,l) < problem.obj_func(b,l))}
	 		var count = 0
			sort_solution(0).foreach{x=> count=count+x.customers.length}
			println("Guided Total cost: "+problem.obj_func(sort_solution(0),l)+ " "+ count+ " "+problem.is_feasible(sort_solution(0)))
	 		best = sort_solution(0)
	 		iter = iter+1
	 	}	
	 	return best
	}
}*/
