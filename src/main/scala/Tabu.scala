import org.apache.spark.SparkContext

class Tabu(define_iter:Int, define_tabu_len:Int, define_change_rate:Double, Start_time:Long) extends Serializable
{
	val iter:Int = define_iter
	val tabu_list_length:Int = define_tabu_len
    val change_rate:Double = define_change_rate
    val start_time:Long = Start_time

	def tabu(f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_id:Array[Courier],id_records:Array[(Int,Record)],points_set:Map[String,Point_id],site_points:Map[String,Point_id],shop_points:Map[String,Point_id],history_tabu_list:Array[String],history_courier_count:Map[String,Int],operator_type:String):(Double,Map[String,Array[String]],Array[(Int,Record)],Array[(Int,Vector)],Array[String],Map[String,Int])=
	{  
		//scala.util.Sorting.stableSort(records, (e1: Record, e2: Record) => e1.Cid < e2.Cid)
		val records = id_records.sortWith{case (a,b)=>
		{
			if (a._2.Cid == b._2.Cid)
				a._1 < b._1
			else
				a._2.Cid < b._2.Cid
		}}.map{x=>x._2}

		var plan_initial=new Plan()
		plan_initial.init(records,points_set,site_points,shop_points,history_tabu_list,tabu_list_length)
		var ans=plan_initial.totalcost(f_orders,e_orders)
		var best_cost=ans._1
		var time_list=ans._2
		var address_list=ans._3
		var type_of_solution=ans._4

		var best_plan=plan_initial.copy_solution()
		var best_time_list=time_list
		var best_address_list=address_list

		var cur_plan=best_plan.copy_solution()
		var new_cost=best_cost
		var pre_cost=best_cost
		var ratio=5000
		var Courier_counter:Map[String,Int]=history_courier_count.toMap
		//c_id.foreach{x=> Courier_counter+=(x.Cid -> 0)}
		println("bestcost "+best_cost+" begin")

        var solution_vector:Array[(Int,Vector)]=Array()

		var cur_iter=1                  
		var max_iter=iter           
		var unchange_iter=0                
		var max_unchange_iter=10           
		var r1=""
		var rc_id=""

		while(cur_iter<=max_iter)
		{
			var get_id=cur_plan.choose_orders(c_id,f_orders,e_orders,0.5,0.5)
			var choose_flag=0
			var candidate:(Int,String,String)=(0,"","")
			var sol:(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=(-1,Array(new Path("")),Map(""->Array(0)),Map(""->Array("")),"")
			for (candidate <- get_id)
			{
				var solution_record=cur_plan.copy_solution()
				if(choose_flag==0)
				{
					r1=candidate._2
					rc_id=candidate._3
					var exist_flag=0
					if(cur_plan.FFI_tabu_list.contains(r1)||cur_plan.EEI_tabu_list.contains(r1)||cur_plan.FFS_inter_tabu_list.contains(r1)||cur_plan.two_tabu_list.contains(r1)||cur_plan.history_tabu_list.contains(r1))
					    exist_flag=1
					//println("r1: "+r1+" rc_id: "+rc_id)
					sol=cur_plan.operator_choose(r1,rc_id,f_orders,e_orders,c_id,ratio,time_list,address_list,operator_type)
					if(exist_flag==1)
					{
						if(sol._1!=(-1)&&sol._1 < best_cost)
							choose_flag=1
						else
							cur_plan=solution_record.copy_solution()
					}
					else
						choose_flag=1
				}
			}
			if(sol._1!=(-1))
			{
				Courier_counter+=(rc_id->(Courier_counter(rc_id)+1))
				new_cost=sol._1
				cur_plan.solution=sol._2.clone()
				time_list=sol._3
				address_list=sol._4
				type_of_solution=sol._5
				if(new_cost < pre_cost)
				{
					unchange_iter=0
					pre_cost=new_cost
				}
				else
					unchange_iter=unchange_iter+1
				if(new_cost < best_cost && type_of_solution=="feasible")
				{
					best_cost=new_cost
					best_plan=cur_plan.copy_solution()
					best_time_list=time_list
					best_address_list=address_list
					unchange_iter=0
				}
			}
			else
				unchange_iter=unchange_iter+1
			if (unchange_iter>=max_unchange_iter)
			{
				var diver_cost=best_cost
				//Courier_counter=ListMap(Courier_counter.toSeq.sortWith(_._2<_._2):_*)
				cur_plan=best_plan.copy_solution()
				time_list=best_time_list
				address_list=best_address_list
				var select_orders=cur_plan.choose_orders(c_id,f_orders,e_orders,0.5,0.5)
				var key_order:List[(Int,Double,String,String)]=List()
				select_orders.foreach{x=>key_order=key_order:+(Courier_counter(x._3),x._1,x._2,x._3)}
				var select_orders_sort=key_order.sortWith((a,b) => (a._1 < b._1))
				var break_flag=0
				var tmp_order:(Int,Double,String,String)=(-1,-1,"","")
				for (tmp_order <- select_orders_sort if break_flag==0)
				{
					//println(tmp_order)
					rc_id=tmp_order._4
					r1=tmp_order._3
					val select_path=cur_plan.select_Path(rc_id)
					if(select_path.index2(r1)._1!=(-1))
					{
						cur_plan.clear_tabu_list()
						sol=cur_plan.operator_choose(r1,rc_id,f_orders,e_orders,c_id,ratio,time_list,address_list,operator_type)
						if(sol._1!=(-1))
						{
							Courier_counter+=(rc_id->(Courier_counter(rc_id)+1))
							pre_cost=sol._1
							new_cost=sol._1
							//println("new_cost: "+new_cost)
							cur_plan.solution=sol._2.clone()
							time_list=sol._3
							address_list=sol._4
							type_of_solution=sol._5
							if(new_cost < best_cost && type_of_solution=="feasible")
							{
								best_cost=new_cost
								best_plan=cur_plan.copy_solution()
								best_time_list=time_list
								best_address_list=address_list
							}
							if(Math.abs(new_cost-diver_cost)/diver_cost >= change_rate)
								break_flag=1
						}
					}
				}
				unchange_iter=0
				cur_plan.clear_tabu_list()
			}
			//println("cur_iter: "+cur_iter+"  "+best_cost+" "+new_cost)
            println((System.nanoTime()-start_time).toDouble/60/1000000000 +" "+best_cost)
            solution_vector = solution_vector :+ (cur_iter,cur_plan.solution_vector(f_orders,e_orders))
			cur_iter+=1
		}
		println("best_cost "+best_cost)
		//best_address_list.keys.foreach{x=>print("Oid:"+x);best_address_list(x).foreach{y=>println(y)}}
		val tabu_list_union = cur_plan.FFI_tabu_list.union(cur_plan.EEI_tabu_list.union(cur_plan.FFS_inter_tabu_list.union(cur_plan.two_tabu_list))).distinct
		return (best_cost,best_address_list,best_plan.transform(),solution_vector,tabu_list_union,Courier_counter)
	}
}
