class Path(Courier_id:String) extends Serializable
{
	val cid = Courier_id
	var orders:Array[Record]=Array()
	var state:Int = 1                 //1:feasible  0:infeasible
	val util=new Util()

	def cost(f_orders:Map[String,Order],e_orders:Map[String,Order_2],penal:Double,E_list:Map[String,String],points:Map[String,Point_id],mode:Int):(Double,Array[Int],Array[String],String)=     //calculate the cost of the current path
    {
    	var time_penalty=0
    	var penalsum=0
    	var now_deptime=0
    	var now_arrtime=0
    	var now_load=0
    	var check:List[String]=List()
    	var recordtime:Array[Int]=Array()
    	var address:Array[String]=Array()
    	val first=orders(0)                                      //the first order in this path
    	var last_spot=" "
    	if(first.Oid.charAt(0)=='E')
    	{
    		var e_tmp=e_orders(first.Oid).fInd
    		last_spot=E_list(e_tmp)
    	}
    	else
    		last_spot=f_orders(first.Oid).fInd

    	var tmp:Record=Record("","")
    	var now_spot:String=""
    	var it=orders.iterator
    	while(it.hasNext)
    	{
    		tmp=it.next()
    		if(tmp.Oid.charAt(0)=='F')                              
    		{
    			var read_order=f_orders(tmp.Oid)
    			if(check.contains(tmp.Oid))
    			{
    				now_spot=read_order.tInd
    				now_arrtime=now_deptime+util.dist(last_spot,now_spot,points)
    				//now_arrtime=now_deptime+distance_matrix(point_map(last_spot))(point_map(now_spot))
    				now_deptime=now_arrtime+util.serv(read_order.num_ord)
    				last_spot=now_spot
    				now_load-=read_order.num_ord
    			}
    			else
    			{
    				now_spot=read_order.fInd
    				now_arrtime=now_deptime+util.dist(last_spot,now_spot,points)
    				//now_arrtime=now_deptime+distance_matrix(point_map(last_spot))(point_map(now_spot))
    				now_deptime=now_arrtime
    				last_spot=now_spot
    				now_load+=read_order.num_ord
    				if (now_load>140)
    					penalsum+=now_load-140
    			}
            }
            if(tmp.Oid.charAt(0)=='E')                             //O2O
            {
            	var read_order=e_orders(tmp.Oid)
            	if(check.contains(tmp.Oid))
            	{
            		now_spot=read_order.tInd
            		now_arrtime=now_deptime+util.dist(last_spot,now_spot,points)
            		//now_arrtime=now_deptime+distance_matrix(point_map(last_spot))(point_map(now_spot))
            		now_deptime=now_arrtime+util.serv(read_order.num_ord)
            		last_spot=now_spot
            		if(now_deptime>read_order.time2)                                       //timeout penalty
            		{
            			time_penalty=time_penalty+5*(now_arrtime-read_order.time2) 
            			now_load-=read_order.num_ord
            		}
                }
            	else
            	{
            		now_spot=read_order.fInd
            		now_arrtime=now_deptime+util.dist(last_spot,now_spot,points)
            		//now_arrtime=now_deptime+distance_matrix(point_map(last_spot))(point_map(now_spot))
            		last_spot=now_spot
            		if(now_arrtime<=read_order.time1)
            			now_deptime=read_order.time1
            		else
            		{
            			now_deptime=now_arrtime
            			time_penalty=time_penalty+5*(now_arrtime-read_order.time1)          //late penalty
            		}
            		now_load+=read_order.num_ord
            		if(now_load>140)
            			penalsum+=now_load-140
            	}
            }
            check=check:+tmp.Oid
            recordtime=recordtime:+now_deptime
            address=address:+last_spot
        }
            //println(check.length)
        var solution_type = ""
        if (penalsum>0)
        {
            solution_type = "infeasible"
            state = 0
        }
        else
        {
            solution_type = "feasible"
            state = 1
        }
        if (mode==0)
            return (time_penalty+now_deptime+penalsum*penal,recordtime,address,solution_type)
        else
            return (now_deptime,recordtime,address,solution_type)
    }

    def empty():Boolean=
    {
    	if (orders.isEmpty)
    		return true
    	else
    		return false
    }

    def next_order(r1:String):String=
    {
    	var idx=index2(r1)._1
    	var cur_order=orders(idx).Oid
    	var next_order=cur_order
    	while(next_order==cur_order)
    	{
    		idx=idx-1
    		if(idx < 0)
    			return "None"
    		next_order=orders(idx).Oid     
    	}
        if(next_order == cur_order)
          return "None"
    	return next_order
    }

    def index2(id:String)=                              //get index of a record whose Oid=id
    {
    	val r1=orders.indexOf(Record(cid,id),0)
    	val r2=orders.indexOf(Record(cid,id),r1+1)
    	(r1,r2)
    }

    def delete_Record(id:String)=                          //remove a record whose Oid=id
    {
    	var new_orders:Array[Record]=Array()
    	new_orders=orders.filter{x=>(x.Oid!=id)}
    	new_orders
    }

    def insert_Record(id:String,index:Int):Array[Record]=                        //insert a record whose Oid=id at index
    {
        if(orders.filter{x=>x.Oid==id}.length ==2)
        {
          return orders
        }
    	var new_orders:Array[Record]=Array()
    	var pos=0
    	for(pos <- 0 to orders.length-1)
    	{
    		if(pos==index) 
    		{
    			new_orders=new_orders:+Record(cid,id)
    			new_orders=new_orders:+orders(pos)
    		} 
    		else 
    			new_orders=new_orders:+orders(pos)
    	}
        if (index >=orders.length)
    		new_orders=new_orders:+Record(cid,id)
    	return new_orders   
    }

    def dist_only(rorder:String,f_orders:Map[String,Order],e_orders:Map[String,Order_2],points:Map[String,Point_id],E_list:Map[String,String]):Int= 
    {
    	var time_penalty=0 
    	var now_deptime=0
    	var rid_check:List[String]=List()
    	var now_arrtime=0
    	var dist_time=0
    	var flag=0
    	var now_spot=""
    	var last_spot=""
    	if (orders(0).Oid.charAt(0)=='F') 
    		last_spot=f_orders(orders(0).Oid).fInd  
    	else
    		last_spot=E_list(e_orders(orders(0).Oid).fInd)
    	var tmp=Record("","")
    	var it=orders.iterator
    	while(it.hasNext)
    	{
    		tmp=it.next()
    		if(tmp.Oid.charAt(0)=='F')
    		{
    			val read_order=f_orders(tmp.Oid)
    			if(rid_check.contains(tmp.Oid))
    				now_spot=read_order.tInd
    			else
    				now_spot=read_order.fInd
    		}
    		if(tmp.Oid.charAt(0)=='E')
    		{
    			val read_order=e_orders(tmp.Oid)
    			if(rid_check.contains(tmp.Oid))
    				now_spot=read_order.tInd
    			else
    				now_spot=read_order.fInd 
    		} 
    		if(flag==1)
    		{
    			dist_time=dist_time+util.dist(last_spot,now_spot,points)
    			flag=0
    		} 
    		if(tmp.Oid==rorder)
    		{
    			dist_time=dist_time+util.dist(last_spot,now_spot,points)
    			flag=1
    		}
    		last_spot=now_spot
    		rid_check=rid_check:+tmp.Oid
    	}
    	return dist_time
    }

    def arc_length(f_orders:Map[String,Order],e_orders:Map[String,Order_2],penal:Double,E_list:Map[String,String],points:Map[String,Point_id]):(Int,Array[Int])=                       //calculate the total length of the current path
    {
        var arc_length_array:Array[Int]=Array()
        var arc_num:Int = 0
        var check:List[String]=List()
        val first=orders(0)                                     
        var last_spot=" "
        if(first.Oid.charAt(0)=='E')
        {
            var e_tmp=e_orders(first.Oid).fInd
            last_spot=E_list(e_tmp)
        }
        else
            last_spot=f_orders(first.Oid).fInd

        var tmp:Record=Record("","")
        var now_spot:String=""
        var it=orders.iterator
        while(it.hasNext)
        {
            tmp=it.next()
            if(tmp.Oid.charAt(0)=='F')                              
            {
                var read_order=f_orders(tmp.Oid)
                if(check.contains(tmp.Oid))
                {
                    now_spot=read_order.tInd
                    val arc_dist = util.dist(last_spot,now_spot,points)
                    if( arc_dist > 0)
                    {
                        arc_num = arc_num + 1
                        arc_length_array = arc_length_array :+ arc_dist
                    }
                    last_spot = now_spot
                }
                else
                {
                    now_spot=read_order.fInd
                    val arc_dist = util.dist(last_spot,now_spot,points)
                    if( arc_dist > 0)
                    {
                        arc_num = arc_num + 1
                        arc_length_array = arc_length_array :+ arc_dist
                    }
                    last_spot=now_spot
                }
            }
            if(tmp.Oid.charAt(0)=='E')                          
            {
                var read_order=e_orders(tmp.Oid)
                if(check.contains(tmp.Oid))
                {
                    now_spot=read_order.tInd
                    val arc_dist = util.dist(last_spot,now_spot,points)
                    if( arc_dist > 0)
                    {
                        arc_num = arc_num + 1
                        arc_length_array = arc_length_array :+ arc_dist
                    }
                    last_spot=now_spot
                }
                else
                {
                    now_spot=read_order.fInd
                    val arc_dist = util.dist(last_spot,now_spot,points)
                    if( arc_dist > 0)
                    {
                        arc_num = arc_num + 1
                        arc_length_array = arc_length_array :+ arc_dist
                    }
                    last_spot=now_spot
                }
            }
            check=check:+tmp.Oid
        }
        return (arc_num,arc_length_array)
    }
  }
