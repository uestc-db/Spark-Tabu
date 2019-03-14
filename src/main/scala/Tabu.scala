import Array._
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

class Tabu(pro:Problem, dura:Double, unchange_iter:Int, tabu_len:Int, pen:Double, inpute_start:Double, dummyheuristics:DummyHeuristics, k:Int) extends Serializable
{
	val problem = pro 
	val heuristics = dummyheuristics
	val duration = dura
	var max_unchange_iter = unchange_iter
	var tabu_length = tabu_len
	var penalty = pen
	var start = inpute_start
	val min_path_num = k
	var cross_tabu_list:ArrayBuffer[(Int,Int)]=ArrayBuffer()
	var insertion_tabu_list:ArrayBuffer[(Int,Int)]=ArrayBuffer()
	var swap_tabu_list:ArrayBuffer[(Int,Int)]=ArrayBuffer()
	var insertion_intra_tabu_list:ArrayBuffer[Int]=ArrayBuffer()
	var swap_pos_tabu_list:ArrayBuffer[(Int,Int)]=ArrayBuffer()
	var one_swap_two_tabu_list:ArrayBuffer[(Int, Int)]=ArrayBuffer()
	var k_neighbors:Map[Int, Array[Int]]=Map()

	def update_tabu_list(mode:Int, number1:Int, number2:Int)
	{
		if (mode == 0)
		{
			cross_tabu_list = cross_tabu_list :+ (number1, number2)
			if(cross_tabu_list.length > tabu_length)
				cross_tabu_list.remove(0)
				//cross_tabu_list = cross_tabu_list.slice(1, cross_tabu_list.length)
		}
		else if (mode == 1)
		{
			insertion_tabu_list = insertion_tabu_list :+ (number1, number2)
			if(insertion_tabu_list.length > tabu_length)
				insertion_tabu_list.remove(0)
				//insertion_tabu_list = insertion_tabu_list.slice(1, insertion_tabu_list.length)
		}
		else if (mode == 2)
		{
			swap_tabu_list = swap_tabu_list :+ (number1, number2)
			if(swap_tabu_list.length > tabu_length)
				swap_tabu_list.remove(0)
				//swap_tabu_list = swap_tabu_list.slice(1, swap_tabu_list.length)
		}
		else if (mode == 3)
		{
			swap_pos_tabu_list = swap_pos_tabu_list :+ (number1, number2)
			if(swap_pos_tabu_list.length > tabu_length)
				swap_pos_tabu_list.remove(0)
				//swap_pos_tabu_list = swap_pos_tabu_list.slice(1, swap_pos_tabu_list.length)
		}
		else if (mode == 4)
		{
			one_swap_two_tabu_list = one_swap_two_tabu_list :+ (number1, number2)
			if(one_swap_two_tabu_list.length > tabu_length)
				one_swap_two_tabu_list.remove(0)
				//one_swap_two_tabu_list = one_swap_two_tabu_list.slice(1, one_swap_two_tabu_list.length)
		}
		else
		{
			insertion_intra_tabu_list = insertion_intra_tabu_list :+ number1
			if(insertion_intra_tabu_list.length > tabu_length)
				insertion_intra_tabu_list.remove(0)
				//insertion_intra_tabu_list = insertion_intra_tabu_list.slice(1, insertion_intra_tabu_list.length)
		}

	}

	def clear_tabu_list()
	{
		cross_tabu_list.clear()
		insertion_tabu_list.clear()
		swap_tabu_list.clear()
		insertion_intra_tabu_list.clear()
		swap_pos_tabu_list.clear()
		one_swap_two_tabu_list.clear()
	}

	def update_penalty(mode:Boolean)          //false:infeasible true:feasible
	{
		if (mode)
			penalty = penalty/(1+0.5)
		else
			penalty = penalty*(1+0.5)
	}

	def is_tabu(mode:Int, number1:Int, number2:Int):Boolean=
	{
		if (mode == 0)
		{
			if (cross_tabu_list.contains((number1,number2)))
				return true
			else
				return false
		}
		else if  ( mode == 1)
		{
			if (insertion_tabu_list.contains((number1,number2)))
				return true
			else
				return false
		}
		else if (mode == 2)
		{
			if (swap_tabu_list.contains((number1,number2)))
				return true
			else
				return false
		}
		else if (mode == 3)
		{
			if (swap_pos_tabu_list.contains((number1,number2)))
				return true
			else
				return false
		}
		else if (mode == 4)
		{
			if (one_swap_two_tabu_list.contains((number1,number2)))
				return true
			else
				return false
		}
		else
		{
			if (insertion_intra_tabu_list.contains(number1))
				return true
			else
				return false
		}

	}

	def one_iter(routes:Array[Route], too_much:Boolean):Array[Route]=
	{
		var moves:Array[(Int, Int, Int, Double ,Int,Route,Route,Int,Int)] = Array()         //(mode, number1, number2, cost, 0:unfeasible/1:feasible, routes,idx1,idx2,pos1,pos2)
		var cur_solution = routes
		var cur_cost = problem.obj_func(cur_solution, penalty)

		var i = 0; var j = 0; var k = 0; var l = 0

		if (routes.length == 1)
		{
			for (i <- 0 to cur_solution.length-1)
			{
				var route = cur_solution(i)
				for( k <- 0 to route.customers.length-1)
				{
					for (j <- 0 to route.customers.length-1)
					{
						val new_route = new Route(heuristics.insertion_intra(route.customers,k,j))
						var flag = 0
						if (new_route.is_feasible(problem))
							flag = 1
						val delta = new_route.total_cost(penalty,problem) - route.total_cost(penalty,problem)
						moves = moves :+ (5, route.customers(k).number, -1, delta ,flag, new_route, new_route,i, i)
					}
				}
			}
		}

		else
		{
			var operator_kind=(new Random).nextInt(5)
			for(i<- 0 to cur_solution.length-1)
			{
				for(j<- 0 to cur_solution.length-1)
				{
					if (i != j )
					{
						val i_route = cur_solution(i)
						val j_route = cur_solution(j)
						for(k <- 0 to cur_solution(i).customers.length-1)
						{
							for (l <- 0 to cur_solution(j).customers.length-1)
							{
								if(!too_much || (too_much && k_neighbors(cur_solution(i).customers(k).number).contains(cur_solution(j).customers(l).number)))
								{
									var c = heuristics.operate(i_route.customers, j_route.customers, k, l,operator_kind)
									var r1 = new Route(c._1)
									var r2 = new Route(c._2)

									var flag = 0
									if (r1.is_feasible(problem) && r2.is_feasible(problem)) 
										flag = 1
									val delta = r1.total_cost(penalty,problem)+r2.total_cost(penalty,problem)-i_route.total_cost(penalty,problem)-j_route.total_cost(penalty,problem)
								moves = moves :+ (operator_kind, i_route.customers(k).number, j_route.customers(l).number, delta, flag, r1,r2,i,j)
								}
							}
						}
					}
				}
			}
		}
		var sort_moves = moves.sortWith{(a,b) => 
			/*if (a._5 == b._5)
				a._4 < b._4
			elsevar new_solution = cur_solution.toBuffer
				a._5 > b._5*/
			a._4 < b._4
		}

		for (i<- 0 to sort_moves.length - 1)
		{
			if (!is_tabu(sort_moves(i)._1, sort_moves(i)._2, sort_moves(i)._3) || (is_tabu(sort_moves(i)._1, sort_moves(i)._2, sort_moves(i)._3) && sort_moves(i)._4 < cur_cost))
			{
				update_tabu_list(sort_moves(i)._1, sort_moves(i)._2, sort_moves(i)._3)
				var select_move = cur_solution.toBuffer
				if (sort_moves(i)._8 == sort_moves(i)._9)
					select_move(sort_moves(i)._8)=sort_moves(i)._6
				else
				{
					select_move(sort_moves(i)._8)=sort_moves(i)._6
					select_move(sort_moves(i)._9)=sort_moves(i)._7
				}
				select_move(sort_moves(i)._8) = refine(select_move(sort_moves(i)._8))
				select_move(sort_moves(i)._9) = refine(select_move(sort_moves(i)._9))
				return select_move.toArray
			}
		}
		return cur_solution
	}

	def refine(route:Route):Route=
	{
		var cur_route = route
		var is_stuckd = false
		while(!is_stuckd)
		{
			is_stuckd = true
			var j =0; var k = 0
			var best = cur_route
			for ( j<- 0 to cur_route.customers.length-1)
			{
				for (k <- 0 to cur_route.customers.length-1)
				{
					val new_route = new Route(heuristics.insertion_intra(cur_route.customers,j,k))
					if ( new_route.total_cost(penalty,problem) < best.total_cost(penalty,problem))
					{
						best = new_route
						is_stuckd = false
					}
				}
			}
			cur_route = best
		}
		return cur_route
	}

	def disturb(routes:Array[Route]):Array[Route]=
	{
		var new_solution = routes.toBuffer
		var i = 0
		for (i <- 0 to new_solution.length-1)
		{
			var route = new_solution(i)
			var rank_routes:Array[(Int, Int, Double)]=Array()
			var k =0;var j = 0
			for( k <- 0 to route.customers.length-1)
			{
				for (j <- k+1 to route.customers.length-1)
				{
					val new_route = new Route(heuristics.two_opt(route.customers,k,j))
					rank_routes = rank_routes :+ ( k, j, new_route.total_cost(penalty, problem))

				}
			}
			if(rank_routes.length != 0)
			{
				val sort_rank_routes = rank_routes.sortWith{(a,b) => a._3 < b._3}
				var denominator	= (1 + sort_rank_routes.length) * sort_rank_routes.length / 2
				var pro:Array[Double]=Array()
				/*for (j <- 0 to sort_rank_routes.length-1)
					pro = ((j+1).toDouble/denominator.toDouble) +: pro                  //big -> small*/
				if (sort_rank_routes.length == 1)
					pro = Array(1.0)
				if (sort_rank_routes.length > 1)
				{
					for ( j <- 0 to sort_rank_routes.length - 1)
					{
						if (j == sort_rank_routes.length-1)
							pro = pro :+ pro(j-1)
						else
							pro = pro :+ (1.0 / Math.pow(2, j+1))
					}
				}

				val intx=(new Random()).nextInt(100000)
	        	val doublex=intx.toDouble/100000.0
	        	var break=0
	        	var cumulative_probability = 0.0
	        	var idx=0
	        	var pos=0
	        	for (idx <- 0 to sort_rank_routes.length-1 if break==0)
	        	{
	            	cumulative_probability=cumulative_probability+pro(idx)
	             	if(doublex < cumulative_probability)
	             	{
	                	pos=idx
	                 	break=1
	             	}
	        	}
	        	new_solution(i) = new Route(heuristics.two_opt(route.customers, sort_rank_routes(pos)._1,sort_rank_routes(pos)._2))
	        }
		}
		return new_solution.toArray
	}

	def disturb_cross(routes:Array[Route]):Array[Route]=
	{
		var cur_solution = routes.toBuffer
		var is_stuckd = false

		while(!is_stuckd)
		{
			is_stuckd = true
			var i = 0;var j =0; var k = 0; var l = 0; var m = 0
			for(i<- 0 to cur_solution.length-1)
			{
				for(j<- 0 to cur_solution.length-1)
				{
					if (i != j)
					{
						var best_i = cur_solution(i)
						var best_j = cur_solution(j)
						for(k <- 0 to cur_solution(i).customers.length-1)
						{
							for (l <- 0 to cur_solution(j).customers.length-1)
							{
								var c = heuristics.operate(cur_solution(i).customers, cur_solution(j).customers, k, l,0)
								var r1 = new Route(c._1)
								var r2 = new Route(c._2)
								if (r1.is_feasible(problem) && r2.is_feasible(problem))
								{
									if (r1.total_cost(0, problem) + r2.total_cost(0,problem) < best_i.total_cost(0,problem) + best_j.total_cost(0, problem))
									{
										best_i = r1
										best_j = r2
										is_stuckd = false
									}
								}
							}
						}
						cur_solution(i)=best_i
			            cur_solution(j)=best_j
					}
				}
			}
			cur_solution=cur_solution.filter{x=>x.customers.length>0}
		}
		return cur_solution.toArray
	}

	def diversification(routes:Array[Route], too_much:Boolean):Array[Route]=
	{
		/*val combination = Array(0,1,2,3,4).permutations.toArray
		var best = disturb(routes).toBuffer
		//for ( operator_kind <- combination(randnum))
		//val candidate_list = heuristics.unimproved_list(best.toArray)
		var randnum=(new Random).nextInt(combination.length)
		for ( operator_kind <- combination(randnum))
		{
			var i = 0;var j =0; var k = 0; var l = 0; var m = 0
			for(i<- 0 to best.length-1)
			{
				for(j<- 0 to best.length-1)
				{
					if (i != j)
					{
						var best_i = best(i)
						var best_j = best(j)
						for(k <- 0 to best(i).customers.length-1)
						{
							for (l <- 0 to best(j).customers.length-1)
							{
								var c = heuristics.operate(best(i).customers, best(j).customers, k, l, operator_kind)
								var r1 = new Route(problem, c._1)
								var r2 = new Route(problem, c._2)
								if (r1.total_cost(penalty) + r2.total_cost(penalty) <= best_i.total_cost(penalty) + best_j.total_cost(penalty))
								{
									best_i = r1
									best_j = r2
									
								}
							}
						}
						best(i)=best_i
						best(j)=best_j			
					}
				}
			}
		}*/
		var cur_solution = disturb_cross(routes)
		cur_solution = disturb(cur_solution)
		//var cur_solution = routes
		var inital_cost:Double = problem.obj_func(cur_solution, penalty)
		var item = 0
		var break_flag = false
		val items = heuristics.unimproved_list(cur_solution.toArray,penalty)
		var randnum=(new Random).nextInt(2)
		var operator_kind = 0
		if (randnum == 0)
			operator_kind = 1
		else
			operator_kind = 3
		for (item <- items if !break_flag)
		{
			//val loc = heuristics.cus_location(cur_solution, item)
			//solution_list = Array()
			var best_cost = 2147483647
			var best_solution = cur_solution
			var i = 0; var j = 0
			var loc_1 = -1; var loc_2 = -1
			for (i <- 0 to cur_solution.length - 1)
			{
				for (j <- 0 to cur_solution(i).customers.length-1)
				{
					if (cur_solution(i).customers(j).number == item)
					{
						loc_1 = i
						loc_2 = j
					}
				}

			}
			if (loc_1!=(-1) && loc_2!=(-1))
			{
				for (i <- 0 to cur_solution.length-1)
				{
					if (i != loc_1)
					{
						for (j <- 0 to cur_solution(i).customers.length-1)
						{
							//if(!too_much || (too_much && problem.k_neighbors(cur_solution(loc_1).customers(loc_2).number).contains(cur_solution(i).customers(j).number)))
							//{
								var c = heuristics.operate(cur_solution(loc_1).customers, cur_solution(i).customers, loc_2, j, operator_kind)
								var r1 = new Route(c._1)
								var r2 = new Route(c._2)
								var new_solution = cur_solution.toBuffer
								new_solution(loc_1)=r1
								new_solution(i)=r2
								val cur_cost:Double = problem.obj_func(new_solution.toArray, penalty)
								if (cur_cost < best_cost)
									best_solution = new_solution.toArray
								//println(inital_cost+" "+ cur_cost+ " "+Math.abs(inital_cost - cur_cost)/inital_cost)
							//}
						}
					}
				}
			}

			cur_solution = best_solution
			if(Math.abs(inital_cost -problem.obj_func(cur_solution, penalty))/inital_cost >= 0.2)
				break_flag = true
		}

		var best = cur_solution.filter{x=> x.customers.length!=0}

		for (i <- 0 to best.length-1)
			best(i) = refine(best(i))

		return best.toArray
	}

	def div(routes:Array[Route], too_much:Boolean):Array[Route]=
	{
		var operator_kind = 0
		var cur_solution = routes
		var solution_list:Array[(Array[Route],Double)]=Array()
		var i = 0; var j = 0; var l = 0; var m = 0;var n = 0
		for (i <- 0 to cur_solution.length-1)
		{
			for (j <- 0 to cur_solution.length - 1)
			{
				if ( i != j)
				{
					for (l <- 0 to cur_solution(i).customers.length - 1)
					{
						for (m <- 0 to cur_solution(j).customers.length - 1)
						{
							for (n <- m+1 to  Math.min(cur_solution(j).customers.length - 1, m+3))
							{
								var c = heuristics.one_swap_two(cur_solution(i).customers, cur_solution(j).customers, l, m, n)
								var r1 = new Route(c._1)
								var r2 = new Route(c._2)

								var new_solution = cur_solution.toBuffer
								new_solution(i)=refine(r1)
								new_solution(j)=refine(r2)

								if(problem.is_feasible(new_solution.toArray))
									solution_list = solution_list :+ (new_solution.toArray, problem.obj_func(new_solution.toArray, penalty))
							}
						}
					}
				}
			}
		}
		val sort_solution_list = solution_list.sortWith{(a,b) => a._2 < b._2}
		if (sort_solution_list.length != 0)
			cur_solution = sort_solution_list(0)._1
		val best = div(cur_solution,too_much).filter{x=>x.customers.length != 0}
		return best
		
	}

	def excute(routes:Array[Route]):(Array[Route],Double)=
	{
		if (routes.length == 0)
		{
			println("Tabu Search Best: "+ ((System.currentTimeMillis()-start).toDouble/60000).toInt + " 0.0 true")
			return (routes,0.0)
		}

		var cur_solution = routes.map{x => x.customers.sortWith((a,b) => a.idx < b.idx); x}
		/*if (cur_solution.length > min_path_num)
			cur_solution = heuristics.merge_solution(cur_solution,penalty)*/

		//if (level != 0)
		//	cur_solution = heuristics.merge_solution(cur_solution,penalty)

		var best = routes
		var best_cost = problem.obj_func(best,penalty)
		var last_cost = best_cost
		var unchange = 0

		var too_much = false
		var count = 0
		cur_solution.foreach{r => count = count + r.customers.length}
		if (count >= 200)
		{
			too_much = true
			var total_customers:Array[POINT]=Array()
			cur_solution.foreach{r => total_customers = concat(total_customers,r.customers)}
			k_neighbors = problem.update_neighbors(total_customers)
		}

        val tabu_start = System.currentTimeMillis()
		var cur_iter = 0
		//while ((System.currentTimeMillis()-tabu_start).toDouble/60000 < duration)
		while(cur_iter < 50)
		{
			var new_solution = one_iter(cur_solution, too_much).filter{x => x.customers.length != 0}
			var new_cost = problem.obj_func(new_solution ,penalty)
			var status = problem.is_feasible(new_solution)
			update_penalty(status)
			if (status && new_cost < best_cost)
			//if(new_cost < best_cost)
			{
				best = new_solution
				best_cost = problem.obj_func(best , penalty)
			}

			if (new_cost <  last_cost)
			{
				last_cost = new_cost
				unchange = 0
			}
			else
			{
				unchange = unchange + 1
				if (unchange >= max_unchange_iter)
				{
					val before = problem.obj_func(new_solution, penalty)
					new_solution = diversification(new_solution,too_much).filter{x => x.customers.length != 0}
					last_cost = problem.obj_func(new_solution, penalty)
					println("before: "+before+" latter: "+last_cost)
					if (last_cost< best_cost && problem.is_feasible(new_solution))
					//if(last_cost < best_cost)
					{
						best = new_solution
						best_cost = last_cost
					}
					unchange = 0
				    clear_tabu_list()
				}
			}

			cur_solution = new_solution
			//println((System.currentTimeMillis()-start).toDouble/60000+" : "+ (System.currentTimeMillis()-tabu_start).toDouble/60000+" : "+problem.obj_func(new_solution) )
			//println("cur_iter "+cur_iter +":"+problem.obj_func(new_solution, penalty))
			cur_iter = cur_iter+ 1
		}
		println("Tabu Search Best: "+ ((System.currentTimeMillis()-start).toDouble/60000).toInt + " " +problem.obj_func(best, penalty)+ " "+  problem.is_feasible(best))

		best.foreach{x => x.set_idx()}
		return (best,problem.obj_func(best, penalty))
	}
}
