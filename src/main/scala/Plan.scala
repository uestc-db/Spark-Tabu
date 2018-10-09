import Array._
import scala.util.Random

class Plan extends Serializable
{
	var solution:Array[Path]=Array()
	var penal:Double=50.0
	var modulation:Double=0.1
	var E_list:Map[String,String]=Map()
	var F_list:Map[String,Array[String]]=Map()
	var FFS_inter_tabu_list:Array[String]=Array()
	var FFI_tabu_list:Array[String]=Array()
	var EEI_tabu_list:Array[String]=Array()
	var two_tabu_list:Array[String]=Array()
	var history_tabu_list:Array[String]=Array()
	var points:Map[String,Point_id]=Map()
	var tabu_list_len:Int=0
    

	def init(records:Array[Record],points_set:Map[String,Point_id],site_points:Map[String,Point_id],shop_points:Map[String,Point_id],his_tabu:Array[String],tabu_list_length:Int)=                               //initialization
    {
    	var util=new Util()
    	var id=records(0).Cid
    	var i=0
    	var path=new Path(id)
    	var len=records.length
    	for (i <-0 to len-1)
    	{
    		if(records(i).Cid==id)
    			path.orders=path.orders:+records(i)
    		else
    		{
    			solution=solution:+path 
    			id=records(i).Cid
    			path=new Path(id) 
    			path.orders=path.orders:+records(i)
    		}
    	}
    	points=points_set
    	solution=solution:+path
    	E_list=util.get_elist(shop_points,site_points,points_set)
    	F_list=util.get_flist(site_points,points_set)
		history_tabu_list = his_tabu.clone()
    	tabu_list_len=tabu_list_length
    }

    def transform():Array[(Int,Record)]=                                //transform the solution to a array of records
    {
    	var id_recordsarray:Array[(Int,Record)]=Array()
		var recordsarray:Array[Record]=Array()
    	solution.foreach{x=>recordsarray=concat(recordsarray,x.orders)}
		var i = 0
		for (i<- 0 to recordsarray.length-1)
			id_recordsarray = id_recordsarray :+ (i,recordsarray(i))
    	return id_recordsarray
    }

    def transform_to_str():String=
    {
    	var solution_str:String=""
    	solution.foreach{path=>
    		solution_str=solution_str+path.cid+":"
    		path.orders.foreach{order=>
    			solution_str=solution_str+order.Oid+","
    		}
    		solution_str=solution_str+";"
    	}
    	return solution_str
    }

    def totalcost(f_orders:Map[String,Order],e_orders:Map[String,Order_2]):(Double,Map[String,Array[Int]],Map[String,Array[String]],String)=               //calculate the cost of the current solution
    {
    	var total=0.0
    	var record_total_time:Map[String,Array[Int]]=Map()
    	var record_total_address:Map[String,Array[String]]=Map()
    	var type_of_solution=""
    	solution.foreach{x=>
        if (x.empty() == false)
        {
        	var ans=x.cost(f_orders,e_orders,penal,E_list,points,0)
        	var cos=ans._1
        	var recotim=ans._2
        	var add=ans._3
        	type_of_solution=ans._4
        	total=total+cos
        	record_total_time+=x.cid->recotim
        	record_total_address+=x.cid->add
        }
		else
		{
			record_total_time+=x.cid->Array()
			record_total_address+=x.cid->Array()
		}}
        return (total,record_total_time,record_total_address,type_of_solution)
    }

    def change(id:String,path:Path):Array[Path]=                                  //replace Path in solution(cid=id) to path
    {
       var changed:Array[Path] = solution.clone()
       changed=changed.filter{x => x.cid != id}
       changed = changed :+ path
       return changed
    }

    def select_Path(id:String)=                                     //return a path of solution
    {
    	var path=new Path(id)
    	solution.foreach{x=>if(x.cid==id) x.orders.foreach{y=>path.orders=path.orders:+y}}
    	path
    }

    def copy_solution():Plan=                                              //solution copy
    {
    	var copy=new Plan()
    	copy.solution=solution.clone()
    	copy.E_list=E_list
    	copy.F_list=F_list
    	copy.FFS_inter_tabu_list=FFS_inter_tabu_list
    	copy.EEI_tabu_list=EEI_tabu_list
    	copy.FFI_tabu_list=FFI_tabu_list
    	copy.two_tabu_list=two_tabu_list.clone()
		copy.history_tabu_list=history_tabu_list.clone()
    	copy.points=points
    	copy.modulation=modulation
        copy.penal=penal
    	copy.tabu_list_len=tabu_list_len
    	return copy
    }

    def clear_tabu_list()=
    {
    	FFS_inter_tabu_list=Array()
    	EEI_tabu_list=Array()
    	FFI_tabu_list=Array()
    	two_tabu_list=Array()
    }

    def order_swap(pathrcid:Path,pathid:Path,rc_id:String,r1:String,id:String,r2:Int):(Path,Path,String)=
    {
    	var path_rcid=new Path(rc_id)
    	var path_id=new Path(id)

    	path_rcid.orders=pathrcid.orders.clone()
    	path_id.orders=pathid.orders.clone()

    	val c1=path_rcid.index2(r1)
        val id_order = path_id.orders(r2).Oid
    	val c2=path_id.index2(id_order)
    	if(c1._1==(-1)||(c1._2)==(-1)||(c2._1)==(-1)||(c2._2)==(-1))
    		return (path_rcid,path_id,"infeasible")
    	if(path_rcid.cid==path_id.cid)
    	{
    		var tmp=path_rcid.orders(c1._1)
    		path_rcid.orders(c1._1)=path_rcid.orders(c2._1)
    		path_rcid.orders(c2._1)=tmp
    		tmp=path_rcid.orders(c1._2)
    		path_rcid.orders(c1._2)=path_id.orders(c2._2)
    		path_rcid.orders(c2._2)=tmp
    		path_id.orders=path_rcid.orders
    	}
    	else
    	{
    		var tmp=Record(path_id.cid,r1)                                          //swap to records
    		path_rcid.orders(c1._1)=Record(path_rcid.cid,id_order)
    		path_id.orders(c2._1)=tmp
    		tmp=Record(path_id.cid,r1)
    		path_rcid.orders(c1._2)=Record(path_rcid.cid,id_order)
    		path_id.orders(c2._2)=tmp      
    	}
        if (path_rcid.orders.filter(x=>x.Oid==r1).length==4 || path_id.orders.filter(x=>x.Oid==id_order).length==4)
          println("order swap error")
    	return (path_rcid,path_id,"feasible")
    }

    def swap(rc_id:String,r1:String,id:String,r2:Int,f_orders:Map[String,Order],e_orders:Map[String,Order_2],move_num:Int):(Int,Double,String)=
    {
    	var path_rcid=select_Path(rc_id)
    	var path_id=select_Path(id)

    	val oldcost=path_rcid.cost(f_orders,e_orders,penal,E_list,points,0)._1+path_id.cost(f_orders,e_orders,penal,E_list,points,0)._1      //The sum of the original cost of the two paths
        
        var next_rcid_order=path_rcid.next_order(r1)
        var next_id_order=path_id.next_order(path_id.orders(r2).Oid)
        var changed_path=order_swap(path_rcid,path_id,rc_id,r1,id,r2)
        path_rcid.orders=changed_path._1.orders.clone()
        path_id.orders=changed_path._2.orders.clone()
        if(changed_path._3=="infeasible")
        	return (-1,0.0,"infeasible")
        if(move_num==2)
        {
        	if(next_rcid_order=="None" || next_id_order=="None")
        		return(-1,0.0,"infeasible")
        	changed_path=order_swap(path_rcid,path_id,rc_id,next_rcid_order,id,path_id.index2(next_id_order)._1)
        	if(changed_path._3=="infeasible")
        		return(-1,0.0,"infeasible")
        	path_rcid.orders=changed_path._1.orders.clone()
        	path_id.orders=changed_path._2.orders.clone()    
        }

        var flag=1
        var rcid_exceed=path_rcid.cost(f_orders,e_orders,penal,E_list,points,1)._1-720
        var id_exceed=path_id.cost(f_orders,e_orders,penal,E_list,points,1)._1-720
        if(rcid_exceed>0 || id_exceed>0)
        	flag=0

        if(flag==1)
        {  
        	var rcid_new=path_rcid.cost(f_orders,e_orders,penal,E_list,points,0)
        	var id_new=path_id.cost(f_orders,e_orders,penal,E_list,points,0)
        	var type_of_solution=""
        	var discost=rcid_new._1+id_new._1-oldcost         //The difference between the cost after exchange and the cost before exchange
        	if(rcid_new._4=="feasible"&&id_new._4=="feasible")
        		type_of_solution="feasible"
        	else
        		type_of_solution="infeasible"
        	if(rc_id==id)
        		discost=discost/2
        	return (0,discost,type_of_solution)
        }
        else
        	return (1,rcid_exceed+id_exceed,"infeasible")                                                                       
    }

    def insert(id:String,removevalue:String,r2:Int,f_orders:Map[String,Order],e_orders:Map[String,Order_2],offset:Int,removevalue2:String):(Int,Double,String)=                                   //offset,The offset between the second record and the first record
    {
    	var select_path=select_Path(id)
        
        val oldcost=select_path.cost(f_orders,e_orders,penal,E_list,points,0)._1
        var flag=1

        select_path.orders=select_path.insert_Record(removevalue,r2)
    
        if(removevalue2.length()>1)
        {
        	select_path.orders=select_path.insert_Record(removevalue,r2)
        	select_path.orders=select_path.insert_Record(removevalue,r2+offset)
        	select_path.orders=select_path.insert_Record(removevalue,r2+offset)
        }
        else
        	select_path.orders=select_path.insert_Record(removevalue,r2+offset)

        var exceed=select_path.cost(f_orders,e_orders,penal,E_list,points,1)._1-720

	    if(exceed>0)
	    	flag=0

	    if(flag==1)
	    {
	    	var id_new=select_path.cost(f_orders,e_orders,penal,E_list,points,0)
	    	var discost=id_new._1-oldcost
	    	var type_of_solution=id_new._4
	    	return (0,discost,type_of_solution)
	    }
	    else
	    	return (1,exceed,"infeasible")
	}

	def choose_orders(c_id:Array[Courier],f_orders:Map[String,Order],e_orders:Map[String,Order_2],alpha:Double,beta:Double):List[(Double,String,String)]=
	{
		var listrecord:Array[String]=Array()
		var r1=0
		var id=Courier("")
		var rcid=""
		var changevalue:Double=0.0
		var find_orders:List[(Double,String,String)]=List()
		var mode=""

		var randnum=(new Random).nextInt(10000)
		if(randnum<=10000)
			mode="cost"
		//else if(randnum>5000 && randnum<=8000)
	    //  mode="distance"
	    else
	    	mode="cost_distance"

        val solution_tmp = copy_solution().solution

	    if(mode == "cost")
	    {
	    	solution_tmp.foreach{path=>
	    		var select=select_Path(path.cid)
	    		var len=select.orders.length
	    		if(len>2)
	    		{
	    			var oldcost=select.cost(f_orders,e_orders,penal,E_list,points,0)._1
	    			var order_list=select.orders.clone()
	    			order_list.foreach{order_select=>
	    				var rorder=order_select.Oid
	    				if(!listrecord.contains(rorder))
	    				{
	    					var c=select.index2(rorder)
	    					select.orders=select.delete_Record(rorder)
	    					var newcost=select.cost(f_orders,e_orders,penal,E_list,points,0)._1
	    					changevalue=oldcost-newcost
	    					listrecord=listrecord:+rorder
	    					find_orders=find_orders:+(changevalue,rorder,select.cid)
	    					select.orders=select.insert_Record(rorder,c._1)
	    					select.orders=select.insert_Record(rorder,c._2)
	    				}
	    			}
	    		}
	    	}
	    }

	    else if(mode == "distance")
	    {
	    	solution_tmp.foreach{path=>
	    		var select=select_Path(path.cid)
	    		var len=select.orders.length
	    		if(len>2)                                                                                                             
	    		{
	    			var order_list=select.orders.clone()
	    			order_list.foreach{order_select=>
	    				var rorder=order_select.Oid
	    				if(!listrecord.contains(rorder))
	    				{
	    					changevalue=select.dist_only(rorder,f_orders,e_orders,points,E_list).toDouble
	    					listrecord=listrecord:+rorder
	    					find_orders=find_orders:+(changevalue,rorder,select.cid)
	    				}
	    			}           
	    		}
	    	}
	    }

	    else
	    {
	    	solution_tmp.foreach{path=>
	    		var select=select_Path(path.cid)
	    		var len=select.orders.length
	    		if(len>2)
	    		{
	    			var oldcost=select.cost(f_orders,e_orders,penal,E_list,points,0)._1
	    			var order_list=select.orders.clone()
	    			order_list.foreach{order_select=>
	    				var rorder=order_select.Oid
	    				if(!listrecord.contains(rorder))
	    				{
	    					val order_dist_only=select.dist_only(rorder,f_orders,e_orders,points,E_list)
	    					var c=select.index2(rorder)
	    					select.orders=select.delete_Record(rorder)
	    					var newcost=select.cost(f_orders,e_orders,penal,E_list,points,0)._1
	    					changevalue=alpha*(oldcost-newcost).toDouble+(beta*order_dist_only).toDouble
	    					listrecord=listrecord:+rorder
	    					find_orders=find_orders:+(changevalue,rorder,select.cid)
	    					select.orders=select.insert_Record(rorder,c._1)
	    					select.orders=select.insert_Record(rorder,c._2)             
	    				}
	    			}        
	    		}
	    	}
	    }

	    val find_orders_sort=find_orders.sortWith((a,b)=>(a._1>b._1))
        //find_orders_sort.foreach{x=> println(x)}
        return find_orders_sort
    }

      def FFS_inter(r1:String,rc_id:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_id:Array[Courier]):(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=
      {
      	var id=Courier("")
      	var r2=0
      	var cost_list:List[Double]=List()
      	var path_list:List[(String,String,String,Int)]=List()
      	var infeasible_cost_list:List[Double]=List()
      	var infeasible_path_list:List[(String,String,String,Int)]=List()
      	var type_list:List[String]=List()
      	var listsame:List[String]=List()
      	var path_id=new Path("")
      	var ans:(Int,Double,String)=(0,0.0,"")
      	for(id<-c_id)
      	{
      		path_id=select_Path(id.Cid)
      		var len=path_id.orders.length
      		for (r2 <- 0 to len-1)
      		{
      			if(path_id.orders(r2).Oid.charAt(0)=='F' && path_id.orders(r2).Oid!=r1 && !listsame.contains(path_id.orders(r2).Oid))
      			{
      				var tmp1=f_orders(path_id.orders(r2).Oid).fInd
      				var tmp2=f_orders(r1).fInd
      				if(F_list(tmp2).contains(tmp1))
      				{
      					ans=swap(rc_id,r1,id.Cid,r2,f_orders,e_orders,1)
      					if(ans._1==0)
      					{
      						cost_list=cost_list:+ans._2
      						path_list=path_list:+(rc_id,r1,id.Cid,r2)
      						type_list=type_list:+ans._3
      					}
      					if(ans._1==1)
      					{
      						infeasible_cost_list=infeasible_cost_list:+ans._2
      						infeasible_path_list=infeasible_path_list:+(rc_id,r1,id.Cid,r2)
      					}
      					listsame=listsame:+path_id.orders(r2).Oid
      				}
      			}
      		}
      	}
      	if (!FFS_inter_tabu_list.contains(r1))
      		FFS_inter_tabu_list=FFS_inter_tabu_list:+r1
      	if(FFS_inter_tabu_list.length>tabu_list_len)
      	{
      		var release_tabu=FFS_inter_tabu_list(0)
      		FFS_inter_tabu_list=FFS_inter_tabu_list.filter{x=> x!=release_tabu}
      	}

     	var cost:Double=0.0
      	var indexvalue:Int=0
      	var rc_id2:String=""
      	var r12:String=""
      	var id2:String=""
      	r2=0
      	var type_of_solution:String=""
      	if(!cost_list.isEmpty)
      	{
      		cost=cost_list.min
      		indexvalue=cost_list.indexOf(cost)
      		rc_id2=path_list(indexvalue)._1
      		r12=path_list(indexvalue)._2
      		id2=path_list(indexvalue)._3
      		r2=path_list(indexvalue)._4
      		type_of_solution=type_list(indexvalue)
      	}
      	else if (!infeasible_cost_list.isEmpty)
      	{
      		cost=infeasible_cost_list.min
      		indexvalue=infeasible_cost_list.indexOf(cost)
      		rc_id2=infeasible_path_list(indexvalue)._1
      		r12=infeasible_path_list(indexvalue)._2
      		id2=infeasible_path_list(indexvalue)._3
      		r2=infeasible_path_list(indexvalue)._4
      		type_of_solution="infeasible"
      	}
      	else
      		return (-1,Array(),Map(),Map(),"")
      	path_id=select_Path(id2)
      	var path_rcid=select_Path(rc_id2)
      	val change_id=path_id.orders(r2).Oid
      	val changed_path=order_swap(path_rcid,path_id,rc_id2,r12,id2,r2)
      	path_rcid=changed_path._1
      	path_id=changed_path._2
      	
      	var solution_tmp=copy_solution()
      	solution_tmp.solution=solution_tmp.change(rc_id2,path_rcid)
      	solution_tmp.solution=solution_tmp.change(id2,path_id)
     	var final_ans=solution_tmp.totalcost(f_orders,e_orders)
     	var best_cost=final_ans._1
      	var best_solution=solution_tmp.solution.clone()
      		
      	val refine1=solution_tmp.Intra_refine(r12,id2,f_orders,e_orders,c_id)
      	if (refine1._1 < best_cost && refine1._1 != (-1)) 
      	{
      		best_cost=refine1._1
      		best_solution=refine1._2.clone()
      		solution_tmp.solution=refine1._2.clone()
      	}
      	val refine2=solution_tmp.Intra_refine(change_id,rc_id2,f_orders,e_orders,c_id)
      	if (refine2._1 < best_cost && refine2._1 != (-1))                                                        
      	{ 
      		best_cost=refine2._1
      		best_solution=refine2._2.clone()  
      		solution_tmp.solution=refine2._2.clone()
      	}
      	solution_tmp.solution=best_solution.clone()
      	final_ans=solution_tmp.totalcost(f_orders,e_orders)
      	return (final_ans._1,best_solution,final_ans._2,final_ans._3,final_ans._4)
    } 

    def EEI(r1:String,rc_id:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_id:Array[Courier],time_list:Map[String,Array[Int]],address_list:Map[String,Array[String]],mode:String):(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=
    {
    	var path_list:List[(Double,String,String,String,Int,Int,String)]=List()
    	var infeasible_path_list:List[(Double,String,String,String,Int,Int)]=List()

    	var solution_tmp=copy_solution()
    	var path_rcid=solution_tmp.select_Path(rc_id)
    	path_rcid.orders=path_rcid.delete_Record(r1)
    	solution_tmp.solution=solution_tmp.change(rc_id,path_rcid)
    	var r1_time1=e_orders(r1).time1

    	solution_tmp.solution.foreach{x=>
    		var select_path=x
    		var r2=0
    		var len=select_path.orders.length
			var time_len = time_list(select_path.cid).length
    		for(r2 <- 0 to len-1)
    		{
				if(r2<=time_len-1)
				{
    			  if(time_list(select_path.cid)(r2)>r1_time1-60 && time_list(select_path.cid)(r2) < r1_time1+60)
    			  {
    				var r3=0
    				for(r3 <- (-r2) to len-r2-1)
    				{
    					var ans=solution_tmp.insert(select_path.cid,r1,r2,f_orders,e_orders,r3,"")
    					if(ans._1==0)
    						path_list=path_list:+(ans._2,rc_id,r1,select_path.cid,r2,r3,ans._3)
    					else
    						infeasible_path_list=infeasible_path_list:+(ans._2,rc_id,r1,select_path.cid,r2,r3)
    				}
				  }
				}
			}
		}
	    if(!EEI_tabu_list.contains(r1))
	    	EEI_tabu_list=EEI_tabu_list:+r1 
	    if(EEI_tabu_list.length>tabu_list_len)
	    {
	    	var release_tabu=EEI_tabu_list(0)
	    	EEI_tabu_list=EEI_tabu_list.filter{x=> x!=release_tabu} 
	    }

		var cost:Double=0.0
		var indexvalue:Int=0
		var rc_id2:String=""
		var r12:String=""
		var id:String=""
		var r2:Int=0
		var r3:Int=0
		var type_of_solution=""

		if(!path_list.isEmpty)
		{
	        path_list=path_list.sortWith{(a,b) => (a._1 < b._1)}
	        if (mode == "second best" && path_list.length >= 2) indexvalue=1 else indexvalue=0
	        cost=path_list(indexvalue)._1
			rc_id2=path_list(indexvalue)._2
			r12=path_list(indexvalue)._3
			id=path_list(indexvalue)._4
			r2=path_list(indexvalue)._5
			r3=path_list(indexvalue)._6
			type_of_solution=path_list(indexvalue)._7
		}
		else if(!infeasible_path_list.isEmpty)
		{
		    infeasible_path_list=infeasible_path_list.sortWith{(a,b)=> (a._1< b._1)}                              
	        if (mode== "second best" && infeasible_path_list.length>=2) indexvalue=1 else indexvalue=0
	        cost=infeasible_path_list(indexvalue)._1
			rc_id2=infeasible_path_list(indexvalue)._2
			r12=infeasible_path_list(indexvalue)._3
			id=infeasible_path_list(indexvalue)._4
			r2=infeasible_path_list(indexvalue)._5
			r3=infeasible_path_list(indexvalue)._6
			type_of_solution="infeasible"
		}
		else
			return (-1,Array(),Map(),Map(),"")
		path_rcid=solution_tmp.select_Path(id)
		path_rcid.orders=path_rcid.insert_Record(r12,r2)
		path_rcid.orders=path_rcid.insert_Record(r12,r2+r3)
		solution_tmp.solution=solution_tmp.change(id,path_rcid)
 
		var ans=solution_tmp.totalcost(f_orders,e_orders)
		return (ans._1,solution_tmp.solution,ans._2,ans._3,ans._4)
	}

	 def FFI(r1:String,rc_id:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_ids:Array[Courier],time_list:Map[String,Array[Int]],address_list:Map[String,Array[String]],mode:String):(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=
	 {
	 	var path_list:List[(Double,String,String,String,Int,Int,String)]=List()
	 	var choose_cid:List[String]=List()
	 	var infeasible_path_list:List[(Double,String,String,String,Int,Int)]=List()

	 	var break_flag=0
	 	var solution_tmp=copy_solution()
	 	var path_rcid=solution_tmp.select_Path(rc_id)
        val records = path_rcid.orders.clone()
	 	path_rcid.orders=path_rcid.delete_Record(r1)
	 	solution_tmp.solution=solution_tmp.change(rc_id,path_rcid)
	 	var r1_fInd=f_orders(r1).fInd

	 	solution_tmp.solution.foreach{x=>
	 		var select_path=x
	 		var r2=0
	 		var len=select_path.orders.length
	 		for(r2 <- 0 to len-1)
	 		{
	 			if(select_path.orders(r2).Oid.charAt(0)=='F' && break_flag==0)
	 			{
	 				var r2_fInd=f_orders(select_path.orders(r2).Oid).fInd
	 				if (F_list(r1_fInd).contains(r2_fInd))
	 				{
	 					choose_cid=choose_cid:+select_path.cid
	 					break_flag=1
	 				}
	 			}
	 		}
	 	}
	 	choose_cid.foreach{x=>
	 		if(x!=rc_id)
	 		{
	 			var path_id=solution_tmp.select_Path(x)
	 			var r2=0
	 			var len =  path_id.orders.length
	 			for(r2 <- 0 to len)
	 			{
	 				var r3=0
	 				for(r3 <- (-r2) to len-1-r2)
	 				{
	 					var ans=solution_tmp.insert(x,r1,r2,f_orders,e_orders,r3,"")
	 					if(ans._1==0)
	 						path_list=path_list:+(ans._2,rc_id,r1,x,r2,r3,ans._3)
	 					else
	 						infeasible_path_list=infeasible_path_list:+(ans._2,rc_id,r1,x,r2,r3)
	 				}
	 			}
	 		}
	 	}
	 	if(!FFI_tabu_list.contains(r1))
	 		FFI_tabu_list=FFI_tabu_list:+r1
	 	if(FFI_tabu_list.length>tabu_list_len)
	 	{
	 		var release_tabu=FFI_tabu_list(0)
	 		FFI_tabu_list=FFI_tabu_list.filter{x=> x!=release_tabu}
	 	}

	 	var cost:Double=0.0
	 	var indexvalue=0
	 	var rc_id2:String=""
	 	var r12:String=""
	 	var id:String=""
	 	var r2:Int=0
	 	var r3:Int=0
	 	var type_of_solution=""

	 	if(!path_list.isEmpty)
	 	{
	 		path_list=path_list.sortWith{(a,b)=> (a._1< b._1)}                              
	 		if (mode== "second best" && path_list.length>=2) indexvalue=1 else indexvalue=0
	 		cost=path_list(indexvalue)._1
	 		rc_id2=path_list(indexvalue)._2
	 		r12=path_list(indexvalue)._3
	 		id=path_list(indexvalue)._4
	 		r2=path_list(indexvalue)._5
	 		r3=path_list(indexvalue)._6
	 		type_of_solution=path_list(indexvalue)._7
	 	}
	 	else if (!infeasible_path_list.isEmpty)
	 	{
	 		infeasible_path_list=infeasible_path_list.sortWith{(a,b)=> (a._1< b._1)}
	 		if (mode== "second best" && infeasible_path_list.length>=2) indexvalue=1 else indexvalue=0
	 		cost=infeasible_path_list(indexvalue)._1
	 		rc_id2=infeasible_path_list(indexvalue)._2
	 		r12=infeasible_path_list(indexvalue)._3
	 		id=infeasible_path_list(indexvalue)._4
	 		r2=infeasible_path_list(indexvalue)._5
	 		r3=infeasible_path_list(indexvalue)._6
	 		type_of_solution="infeasible"
	 	}
	 	else
	 		return (-1,Array(),Map(),Map(),"")
	 	path_rcid=solution_tmp.select_Path(id)
        val records2 = path_rcid.orders.clone()
	 	path_rcid.orders=path_rcid.insert_Record(r12,r2)
	 	path_rcid.orders=path_rcid.insert_Record(r12,r2+r3)
	 	solution_tmp.solution=solution_tmp.change(id,path_rcid)
      
	 	var ans=solution_tmp.totalcost(f_orders,e_orders)
	 	return (ans._1,solution_tmp.solution,ans._2,ans._3,ans._4)
	}

	def Swap_2_2(r1:String,rc_id:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_id:Array[Courier]):(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=
	{
		var id=Courier("")
		var r2=0
		var cost_list:List[Double]=List()
		var path_list:List[(String,String,String,Int)]=List()
		var infeasible_cost_list:List[Double]=List()
		var infeasible_path_list:List[(String,String,String,Int)]=List()
		var type_list:List[String]=List()
		var listsame:List[String]=List()
		var path_id=new Path("")
		var ans:(Int,Double,String)=(0,0.0,"")

        //var solution_tmp=copy_solution()
		var path_rcid=select_Path(rc_id)

		if(path_rcid.index2(r1)._1<=0 || path_rcid.next_order(r1)=="None")
		{
			if(!two_tabu_list.contains(r1))
				two_tabu_list=two_tabu_list:+r1
			if(two_tabu_list.length>tabu_list_len)
			{
				var release_tabu=two_tabu_list(0)
				two_tabu_list=two_tabu_list.filter{x=> x!=release_tabu}        
			}
			return (-1,Array(),Map(),Map(),"")
		}
    	for(id<-c_id)
      	{
      		path_id=select_Path(id.Cid)
      		var len=path_id.orders.length
      		for (r2 <- 0 to len-1)
      		{
      			if(path_id.orders(r2).Oid.charAt(0)=='F' && path_id.orders(r2).Oid!=r1 && !listsame.contains(path_id.orders(r2).Oid))
      			{
      				ans=swap(rc_id,r1,id.Cid,r2,f_orders,e_orders,2)
      				if(ans._1==0)
      				{
      					cost_list=cost_list:+ans._2
      					path_list=path_list:+(rc_id,r1,id.Cid,r2)
      					type_list=type_list:+ans._3
      				}
      				if(ans._1==1)
      				{
      					infeasible_cost_list=infeasible_cost_list:+ans._2
      					infeasible_path_list=infeasible_path_list:+(rc_id,r1,id.Cid,r2)
      				}
      				listsame=listsame:+path_id.orders(r2).Oid
  
      			}
      		}
      	}

		if(!two_tabu_list.contains(r1))
			two_tabu_list=two_tabu_list:+r1 
		if(two_tabu_list.length>tabu_list_len)
		{
			var release_tabu=two_tabu_list(0)
			two_tabu_list=two_tabu_list.filter{x=> x!=release_tabu}
		}

		var cost:Double=0.0
      	var indexvalue:Int=0
      	var rc_id2:String=""
      	var r12:String=""
      	var id2:String=""
      	r2=0
      	var type_of_solution:String=""
      	if(!cost_list.isEmpty)
      	{
      		cost=cost_list.min
      		indexvalue=cost_list.indexOf(cost)
      		rc_id2=path_list(indexvalue)._1
      		r12=path_list(indexvalue)._2
      		id2=path_list(indexvalue)._3
      		r2=path_list(indexvalue)._4
      		type_of_solution=type_list(indexvalue)
      	}
      	else if (!infeasible_cost_list.isEmpty)
      	{
      		cost=infeasible_cost_list.min
      		indexvalue=infeasible_cost_list.indexOf(cost)
      		rc_id2=infeasible_path_list(indexvalue)._1
      		r12=infeasible_path_list(indexvalue)._2
      		id2=infeasible_path_list(indexvalue)._3
      		r2=infeasible_path_list(indexvalue)._4
      		type_of_solution="infeasible"
      	}
      	else
      		return (-1,Array(),Map(),Map(),"")
      	
        path_id=select_Path(id2)
      	path_rcid=select_Path(rc_id2)
        val change_id=path_id.orders(r2).Oid
        val next_rcid_order=path_rcid.next_order(r12)
        val next_id_order=path_id.next_order(change_id)
      	var changed_path=order_swap(path_rcid,path_id,rc_id2,r12,id2,r2)
      	path_rcid=changed_path._1
      	path_id=changed_path._2
      	
		changed_path=order_swap(path_rcid,path_id,rc_id2,next_rcid_order,id2,path_id.index2(next_id_order)._1)
		path_rcid=changed_path._1
		path_id=changed_path._2  

        var solution_tmp=copy_solution()
		solution_tmp.solution=solution_tmp.change(rc_id2,path_rcid)
		solution_tmp.solution=solution_tmp.change(id2,path_id)

		var final_ans=solution_tmp.totalcost(f_orders,e_orders)
		var best_cost=final_ans._1
		var best_solution=solution_tmp.solution.clone()

	    val refine1=solution_tmp.Intra_refine(r12,id2,f_orders,e_orders,c_id)
		if (refine1._1 < best_cost && refine1._1 != (-1))
		{
			best_cost=refine1._1
			best_solution=refine1._2.clone()
			solution_tmp.solution=refine1._2.clone()
		}
		val refine2=solution_tmp.Intra_refine(next_rcid_order,id2,f_orders,e_orders,c_id)
		if (refine2._1 < best_cost && refine2._1 != (-1))
		{
			best_cost=refine2._1                    
			best_solution=refine2._2.clone()
			solution_tmp.solution=refine2._2.clone()
		}

		val refine3=solution_tmp.Intra_refine(change_id,rc_id2,f_orders,e_orders,c_id)
		if (refine3._1 < best_cost && refine3._1 != (-1))
		{
			best_cost=refine3._1                    
			best_solution=refine3._2.clone()
			solution_tmp.solution=refine3._2.clone()
		}
		val refine4=solution_tmp.Intra_refine(next_id_order,rc_id2,f_orders,e_orders,c_id)
		if (refine4._1 < best_cost  && refine4._1 != (-1))
		{
			best_cost=refine4._1                    
			best_solution=refine4._2.clone()
			solution_tmp.solution=refine4._2.clone()
		}

		solution_tmp.solution=best_solution.clone()
   
		final_ans=solution_tmp.totalcost(f_orders,e_orders)
		return (final_ans._1,solution_tmp.solution,final_ans._2,final_ans._3,final_ans._4)
	}


	def Insert_2_2(r1:String,rc_id:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_id:Array[Courier]):(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)= 
	{
		var cost_list:List[Double]=List()
		var path_list:List[(String,String,String,Int,Int)]=List()
		var type_list:List[String]=List()
		var infeasible_cost_list:List[Double]=List()
		var infeasible_path_list:List[(String,String,String,Int,Int)]=List()
		var solution_tmp=copy_solution()
		var path_rcid=solution_tmp.select_Path(rc_id)

		var next_rcid_order=path_rcid.next_order(r1)
		if(next_rcid_order=="None")
		{
			if(!two_tabu_list.contains(r1))
				two_tabu_list=two_tabu_list:+r1
			if(two_tabu_list.length>tabu_list_len)
			{
				var release_tabu=two_tabu_list(0)
				two_tabu_list=two_tabu_list.filter{x=> x!=release_tabu}          
			}
			return (-1,Array(),Map(),Map(),"")       
		}
        val ori_len = path_rcid.orders.length
		path_rcid.orders=path_rcid.delete_Record(r1)                                      
		path_rcid.orders=path_rcid.delete_Record(next_rcid_order)
        val delta =ori_len -  path_rcid.orders.length
		solution_tmp.solution=solution_tmp.change(rc_id,path_rcid)

		solution_tmp.solution.foreach{x=>
			var select_path=x
			var r2=0
			var len=select_path.orders.length
			for(r2 <- 0 to len-1)
			{
				var r3=0
				var ans=solution_tmp.insert(select_path.cid,r1,r2,f_orders,e_orders,r3,next_rcid_order)
				if(ans._1==0)
				{
					cost_list=cost_list:+ans._2
					path_list=path_list:+(rc_id,r1,select_path.cid,r2,r3)
					type_list=type_list:+ans._3                   
				}
				else                                                                                                                                                                    
				{
					infeasible_cost_list=infeasible_cost_list:+ans._2
					infeasible_path_list=infeasible_path_list:+(rc_id,r1,select_path.cid,r2,r3)                   
				}                        
			}
		}

		if(!two_tabu_list.contains(r1))
			two_tabu_list=two_tabu_list:+r1
		if(two_tabu_list.length>tabu_list_len)
		{
			var release_tabu=two_tabu_list(0)
			two_tabu_list=two_tabu_list.filter{x=> x!=release_tabu}       
		}

		var cost:Double=0.0
		var indexvalue:Int=0
		var rc_id2:String=""                                                                
		var r12:String=""
		var id2:String=""
		var r2:Int=0
		var r3:Int=0
		var type_of_solution=""

		if(!cost_list.isEmpty)
		{
			cost=cost_list.min
			indexvalue=cost_list.indexOf(cost)
			rc_id2=path_list(indexvalue)._1
			r12=path_list(indexvalue)._2
			id2=path_list(indexvalue)._3                                                                                                                                                     
			r2=path_list(indexvalue)._4
			r3=path_list(indexvalue)._5
			type_of_solution=type_list(indexvalue)
		}
		else if(!infeasible_cost_list.isEmpty)
		{
			cost=infeasible_cost_list.min
			indexvalue=infeasible_cost_list.indexOf(cost)
			rc_id2=infeasible_path_list(indexvalue)._1
			r12=infeasible_path_list(indexvalue)._2
			id2=infeasible_path_list(indexvalue)._3
			r2=infeasible_path_list(indexvalue)._4 
			r3=infeasible_path_list(indexvalue)._5
			type_of_solution="infeasible"       
		}
		else
			return (-1,Array(),Map(),Map(),"")


		path_rcid=solution_tmp.select_Path(id2)
        val ori_len1 = path_rcid.orders.length
		path_rcid.orders=path_rcid.insert_Record(r12,r2)
		path_rcid.orders=path_rcid.insert_Record(r12,r2)
		path_rcid.orders=path_rcid.insert_Record(next_rcid_order,r2+r3)
		path_rcid.orders=path_rcid.insert_Record(next_rcid_order,r2+r3)
        val delta1 = path_rcid.orders.length - ori_len1
		solution_tmp.solution=solution_tmp.change(id2,path_rcid)

		var ans=solution_tmp.totalcost(f_orders,e_orders)
		var best_cost=ans._1
		var best_solution=solution_tmp.solution.clone()

		val refine1=solution_tmp.Intra_refine(r12,id2,f_orders,e_orders,c_id)
		if (refine1._1 < best_cost && refine1._1 != (-1))
		{
			best_cost=refine1._1
			best_solution=refine1._2.clone()
			solution_tmp.solution=refine1._2.clone()
		}
		val refine2=solution_tmp.Intra_refine(next_rcid_order,id2,f_orders,e_orders,c_id)
		if (refine2._1 < best_cost && refine2._1 != (-1))
		{
			best_cost=refine2._1
			best_solution=refine2._2.clone()
			solution_tmp.solution=refine2._2.clone()
		}

		solution_tmp.solution=best_solution.clone()

		ans=solution_tmp.totalcost(f_orders,e_orders)
		return (ans._1,solution_tmp.solution,ans._2,ans._3,ans._4)
	}

	def Intra_refine(r1:String,rc_id:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_id:Array[Courier]):(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=
	{
		var cost_list:List[Double]=List()
		var path_list:List[(String,Int,Int)]=List()
		var type_list:List[String]=List()
		var infeasible_cost_list:List[Double]=List()
		var infeasible_path_list:List[(String,Int,Int)]=List()
        

		var path_rcid=select_Path(rc_id)
        val indexs = path_rcid.index2(r1)
        if(indexs._1 == (-1))
          return (-1,Array(),Map(),Map(),"")
		path_rcid.orders=path_rcid.delete_Record(r1)

		var solution_tmp=copy_solution()
		solution_tmp.solution=solution_tmp.change(rc_id,path_rcid)

		var len=path_rcid.orders.length
		var r2=0
		for (r2<- 0 to len-1)
		{
			var r3=0
			for (r3 <- (-r2) to len-r2-1)
			{
				var ans=solution_tmp.insert(rc_id,r1,r2,f_orders,e_orders,r3,"")
				if(ans._1==0)
				{
					cost_list=cost_list:+ans._2
					path_list=path_list:+(r1,r2,r3)
					type_list=type_list:+ans._3
				}
				else
				{
					infeasible_cost_list=infeasible_cost_list:+ans._2
					infeasible_path_list=infeasible_path_list:+(r1,r2,r3)
				}
			}
		}

		var cost:Double=0.0
		var indexvalue:Int=0
		var rc_id2:String=""
		var r12:String=""
		var r22:Int=0
		var r32:Int=0
		var type_of_solution=""

		if(!cost_list.isEmpty)
		{
			cost=cost_list.min
			indexvalue=cost_list.indexOf(cost)
			r12=path_list(indexvalue)._1
			r22=path_list(indexvalue)._2
			r32=path_list(indexvalue)._3
			type_of_solution=type_list(indexvalue)
		}
		else if(!infeasible_cost_list.isEmpty)
		{
			cost=infeasible_cost_list.min
			indexvalue=infeasible_cost_list.indexOf(cost)
			r12=infeasible_path_list(indexvalue)._1
			r22=infeasible_path_list(indexvalue)._2
			r32=infeasible_path_list(indexvalue)._3
			type_of_solution="infeasible"
		}
		else
			return (-1,Array(),Map(),Map(),"")

		path_rcid.orders=path_rcid.insert_Record(r12,r22)
		path_rcid.orders=path_rcid.insert_Record(r12,r22+r32)
		solution_tmp.solution=solution_tmp.change(rc_id,path_rcid)
		
		var ans=solution_tmp.totalcost(f_orders,e_orders)
		return (ans._1,solution_tmp.solution,ans._2,ans._3,ans._4)
	}
    
    def operator_choose(r1:String,rc_id:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_id:Array[Courier],ratio:Int,time_list:Map[String,Array[Int]],address_list:Map[String,Array[String]],operator_mode:String):(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=
	{
		if(operator_mode == "type_1")
		{
			if(r1.charAt(0)=='F')
			{
				var randnum=(new Random).nextInt(10000)
				if(randnum<=8000)
					return FFI(r1,rc_id,f_orders,e_orders,c_id,time_list,address_list,"best")
				else
					return Insert_2_2(r1,rc_id,f_orders,e_orders,c_id)

			}
			else
			{
				var randnum=(new Random).nextInt(10000)
				if(randnum<=8000)
					return EEI(r1,rc_id,f_orders,e_orders,c_id,time_list,address_list,"best")
				else
					return Insert_2_2(r1,rc_id,f_orders,e_orders,c_id)
			}
		}

		else if(operator_mode == "type_2")
		{
			var randnum=(new Random).nextInt(10000)
			if(randnum<=5000)
				return Swap_2_2(r1,rc_id,f_orders,e_orders,c_id)
			else
				return Insert_2_2(r1,rc_id,f_orders,e_orders,c_id)
		}

		else if (operator_mode == "type_3")
		{
			var randnum=(new Random).nextInt(10000)
			if(r1.charAt(0)=='F')
			{
				var path_rcid=select_Path(rc_id)
				if(r1==path_rcid.orders(0).Oid && path_rcid.orders(1).Oid.charAt(0)=='E')
					return FFS_inter(r1,rc_id,f_orders,e_orders,c_id)
				else
					return Swap_2_2(r1,rc_id,f_orders,e_orders,c_id)
			}
			else
				return Swap_2_2(r1,rc_id,f_orders,e_orders,c_id)

		}

		else
		{
			if(r1.charAt(0)=='F')
			{
				var path_rcid=select_Path(rc_id)
				if(r1==path_rcid.orders(0).Oid && path_rcid.orders(1).Oid.charAt(0)=='E')
					return FFS_inter(r1,rc_id,f_orders,e_orders,c_id)
				var randnum=(new Random).nextInt(10000)
				if(randnum<=8000)
					return FFI(r1,rc_id,f_orders,e_orders,c_id,time_list,address_list,"best")
				/*else if(randnum>3000 && randnum<=6000)
				{
					//println("FFI second best")               
					return FFI(r1,rc_id,f_orders,e_orders,c_id,time_list,address_list,"second best")
				}*/
				else if(randnum>8000 && randnum<=9000)
					return Swap_2_2(r1,rc_id,f_orders,e_orders,c_id)
				else
					return Insert_2_2(r1,rc_id,f_orders,e_orders,c_id)
			}
			else
			{
				var randnum=(new Random).nextInt(10000)
				if(randnum<=8000)
					return EEI(r1,rc_id,f_orders,e_orders,c_id,time_list,address_list,"best")           
				/*else if(randnum>4000 && randnum>=8000)
				{
					//println("EEI second best")               
					return EEI(r1,rc_id,f_orders,e_orders,c_id,time_list,address_list,"second best")
				}*/
				else if(randnum>8000 && randnum<=9000)
					return Swap_2_2(r1,rc_id,f_orders,e_orders,c_id)           
				else
					return Insert_2_2(r1,rc_id,f_orders,e_orders,c_id)          
			}
		}
    }
    
    def get_ramdon_solution(f_orders:Map[String,Order],e_orders:Map[String,Order_2],c_ids:Array[Courier]):Array[Path]=
    {
	  var return_solution=copy_solution()
	  val change_count=new Random().nextInt(6)+5
	  var sol:(Double,Array[Path],Map[String,Array[Int]],Map[String,Array[String]],String)=(-1,Array(new Path("")),Map(""->Array(0)),Map(""->Array("")),"")
	  var type_of_solution:String=""
	  val time_list:Map[String,Array[Int]]=Map()
	  val address_list:Map[String,Array[String]]=Map()

      var i=0
	  for(i <- 1 to change_count)
	  {
		  val get_id=(return_solution.choose_orders(c_ids,f_orders,e_orders,0.5,0.5)).filter{x=> x._2.charAt(0)=='F'}
		  var r1=get_id(0)._2
		  var rc_id=get_id(0)._3
		  var select_type=new Random().nextInt(2)
		  
		  if(select_type == 0)
			  sol=return_solution.FFS_inter(r1,rc_id,f_orders,e_orders,c_ids)
		  if(select_type == 1)
			  sol=return_solution.FFI(r1,rc_id,f_orders,e_orders,c_ids,time_list,address_list,"best")
		  if(sol._1!=(-1))
		  {
			  return_solution.solution=sol._2.clone()
			  type_of_solution=sol._5
		  }
		  return_solution.clear_tabu_list()
	  }
	  while(type_of_solution=="infeasible")
	  {
		  val get_id=(return_solution.choose_orders(c_ids,f_orders,e_orders,0.5,0.5)).filter{x=> x._2.charAt(0)=='F'}
		  val r1=get_id(0)._2
		  val rc_id=get_id(0)._3
		  var select_type=new Random().nextInt(2)
		  
		  if(select_type == 0)
			  sol=return_solution.FFS_inter(r1,rc_id,f_orders,e_orders,c_ids)
		  if(select_type == 1)
			  sol=return_solution.FFI(r1,rc_id,f_orders,e_orders,c_ids,time_list,address_list,"best")
		  if(sol._1!=(-1))
		  {
			  return_solution.solution=sol._2.clone()
			  type_of_solution=sol._5
		  }
		  return_solution.clear_tabu_list()
	  }
	  return return_solution.solution
    }

    def solution_vector(f_orders:Map[String,Order],e_orders:Map[String,Order_2]):Vector=
    {
      var total_cost:Double = 0.0
      var max_customer_num:Int = 0
      var median_customer_num:Int = 0
      var min_customer_num:Int = 0
      var total_path_num:Int = 0
      var ave_length:Int = 0
      var ave_arc_length:Int = 0
      var ave_cost:Double = 0

      total_cost = totalcost(f_orders,e_orders)._1

      solution.foreach{path =>
        if (!path.empty())
          total_path_num = total_path_num + 1
      }

      ave_cost = total_cost / total_path_num.toDouble

      var customer_num:Array[Int]=Array()
      solution.foreach{path =>
          if (!path.empty())
            customer_num = customer_num :+ path.orders.length/2

      }
      max_customer_num = customer_num.max
      min_customer_num = customer_num.min
      val (lower, upper) = customer_num.sortWith(_<_).splitAt(customer_num.size / 2)
      if (customer_num.size % 2 == 0) median_customer_num = (lower.last + upper.head) / 2 else median_customer_num = upper.head

      var arc_array:Array[Int]=Array()
      var arc_num = 0
      solution.foreach{path =>
        if (!path.empty())
        {
          val get_arc_length = path.arc_length(f_orders,e_orders,penal,E_list,points)
          arc_array = concat(arc_array,get_arc_length._2)
          arc_num = arc_num + get_arc_length._1
        } 
      }

      val total_length = arc_array.reduce(_+_)
      ave_length = total_length/total_path_num
      ave_arc_length = total_length/arc_num

      return Vector(total_cost,max_customer_num,median_customer_num,min_customer_num,total_path_num,ave_length,ave_arc_length,ave_cost)
    }
}


