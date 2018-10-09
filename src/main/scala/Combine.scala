import Array._

class Combine extends Serializable
{
  val util_define=new Util()

  def get_pairs(recs:Array[(Int,(Double,Double,Double,Double))]):Map[Int,Int]=                          //Pair partitions
  {
    var recs_array:Array[Array[(Int,(Double,Double,Double,Double))]]=Array()
    if(recs.length > 16)
      recs_array = divide_recs(recs)
    else
      recs_array = Array(recs)

    var key_id:Map[Int,Int] = Map()
    recs_array.foreach{recs => 
      val new_pairs = get_pairs_divide(recs)
      val offset = key_id.toArray.length
      new_pairs.foreach{p => key_id += (p._1 -> (p._2+offset))}}
    return key_id
  }

  def get_pairs_divide(recs:Array[(Int,(Double,Double,Double,Double))]):Map[Int,Int]=
  {
    var return_pairs:Array[(Int,Int)]=Array()
    var diff_pairs:Array[(Int,(Double,Array[(Int,Int)]))]=Array()                  //separate rec, (distance, grouped rec)
    var single=(-1)
    if(recs.length%2==0)
      return_pairs=get_pairs_even(recs)._2
    else
    {
      var pick_rec = (0.0,0.0,0.0,0.0)
      for(pick_rec <- recs)
      {
        var recs_even=recs.filter{x=>x!=pick_rec}
        diff_pairs=diff_pairs:+(pick_rec._1,get_pairs_even(recs_even))
      }
      var diff_pairs_sort = diff_pairs.sortWith{(a,b) => (a._2._1 < b._2._1)}                 //sort by distance
      single = diff_pairs_sort(0)._1
      return_pairs = diff_pairs_sort(0)._2._2
    }
    var key_id:Map[Int,Int] = Map()
    return_pairs.foreach{x =>
      key_id+=(x._1->return_pairs.indexOf(x))
      key_id+=(x._2->return_pairs.indexOf(x))}
    if(single!=(-1))
      key_id+=(single->(-1))
    //key_id.foreach{x => println(x._1 +" "+x._2)}
    return key_id
  }

  def divide_recs(recs:Array[(Int,(Double,Double,Double,Double))]):Array[Array[(Int,(Double,Double,Double,Double))]]=
  {
    var min_lat = recs(0)._2._2
    var min_lng = recs(0)._2._3
    var based_rec_idx = 0   
    var recs_array1:Array[(Int,(Double,Double,Double,Double))]=Array()
    var recs_array2:Array[(Int,(Double,Double,Double,Double))]=Array()
    var recs_array:Array[Array[(Int,(Double,Double,Double,Double))]]=Array()
    recs.foreach{rec =>
      if (rec._2._2 <= min_lat && rec._2._3 <= min_lng)
      {
        min_lat = rec._2._2
        min_lng = rec._2._3
        based_rec_idx = recs.indexOf(rec)
      }
    }
    val based_rec = recs(based_rec_idx)._2
    var dist_recs = recs.map{x => (rec_distance(x._2,based_rec),x)}.sortWith{(a, b) => a._1 < b._1}
    var i =0
    for( i <- 0 to 15 )
      recs_array1 = recs_array1 :+ dist_recs(i)._2
    for (i <- 16 to recs.length-1)
      recs_array2 = recs_array2 :+ dist_recs(i)._2

    if(recs_array2.length > 16)
      recs_array = divide_recs(recs_array2)
    else
      recs_array = recs_array :+ recs_array2

    return (recs_array :+ recs_array1)
  }

  def get_pairs_even(recs:Array[(Int,(Double,Double,Double,Double))]):(Double,Array[(Int,Int)])=               //when there are even numbers of partitions
  {
    val Maxnum = 20
    var dist=ofDim[Double](Maxnum,Math.pow(2,Maxnum).toInt)
    var pair=ofDim[Array[(Int,Int)]](Maxnum,Math.pow(2,Maxnum).toInt)
    var i=0;var j=0;var s=0
    for (i <- 0 to Maxnum-1)
      for(j <- 0 to Math.pow(2,Maxnum).toInt-1)
        pair(i)(j)=Array()
    for (i <- 0 to (recs.length)-1)
    {
      for (s <- 0 to (Math.pow(2,(i+1)).toInt-1))
      {
        //println(i + " "+s)
        if (s==0) dist(i)(s)=0
        else dist(i)(s)=1e10
        if((s&(Math.pow(2,i).toInt))!=0)
        {
          for (j <- (0 to i-1).reverse)
          {
            if((s&(Math.pow(2,j).toInt))!=0)
            {
              var d=rec_distance(recs(i)._2,recs(j)._2)
              var tmp=d+dist(i-1)(s^Math.pow(2,i).toInt^Math.pow(2,j).toInt)
              if(dist(i)(s) < tmp)
                dist(i)(s)=dist(i)(s)
              else
                dist(i)(s)=tmp
              if(dist(i)(s) == tmp)
                pair(i)(s)=pair(i-1)(s^Math.pow(2,i).toInt^Math.pow(2,j).toInt):+(recs(i)._1,recs(j)._1)
              else
                pair(i)(s)=pair(i)(s)
            }
          }
        }
        else if(i!=0)
        {
          dist(i)(s)=dist(i-1)(s)
          pair(i)(s)=pair(i-1)(s)
        }
      }
    }
    return (dist(recs.length-1)(Math.pow(2,recs.length).toInt-1),pair(recs.length-1)(Math.pow(2,recs.length).toInt-1))
  }

  def rec_bound(id_point_list:Array[(Int,Record)],f_orders:Map[String,Order], e_orders:Map[String,Order_2],points_set:Map[String,Point_id]):(Double,Double,Double,Double)=                       //get MBR of a solution
  {
    var up=(-90.0)
    var down=90.0
    var left=180.0
    var right=(-180.0)
    var point_set_map:Map[String,Point_id]=Map()
    var address_list:Array[String]=Array()
	val point_list = id_point_list.map{x=>x._2}
    point_list.foreach{point => 
      if(point.Oid.charAt(0)=='F')
      {
        address_list = address_list :+ f_orders(point.Oid).fInd
        address_list = address_list :+ f_orders(point.Oid).tInd
      }
      else
      {
        address_list = address_list :+ e_orders(point.Oid).fInd
        address_list = address_list :+ e_orders(point.Oid).tInd
      }
    }
    address_list = address_list.distinct
    address_list.foreach{site=>               //site
        var tmp=points_set(site)
        if (tmp.lat > up) up=tmp.lat
        if (tmp.lat < down) down=tmp.lat
        if (tmp.lng < left) left=tmp.lng
        if (tmp.lng > right) right=tmp.lng
    }
    return (up,down,left,right)
  }

  def rec_distance(rec1:(Double,Double,Double,Double),rec2:(Double,Double,Double,Double)):Double=           //calculate the distance between two recs
  {
    var rec_distance=0.0                                        //rec:(up,down,left,right)
    if(rec2._1 < rec1._2)
    {
      if(rec2._3 > rec1._4)         //upper right
        rec_distance=util_define.point_dist(rec1._2,rec1._4,rec2._1,rec2._3)
      if(rec2._4 < rec1._3)          //upper left
        rec_distance=util_define.point_dist(rec1._2,rec1._3,rec2._1,rec2._4)
      if(rec2._3 <= rec1._4 && rec2._3 >= rec1._3)       //right below
        rec_distance=util_define.point_dist(rec1._2,rec2._3,rec2._1,rec2._3)
      if(rec2._4 <= rec1._4 && rec2._4 >= rec1._3)          //left below
        rec_distance=util_define.point_dist(rec1._2,rec2._4,rec2._1,rec2._4)
    }
    if(rec2._2 > rec1._1)
    {
      if(rec2._3 > rec1._4)            //upper right
        rec_distance=util_define.point_dist(rec1._1,rec1._4,rec2._2,rec2._3)
      if(rec2._4 < rec1._3)             //upper left
        rec_distance=util_define.point_dist(rec1._1,rec1._3,rec2._2,rec1._4)
      if(rec2._3 <= rec1._4 && rec2._3 >= rec1._3)                     
        rec_distance=util_define.point_dist(rec1._1,rec2._3,rec2._2,rec2._3)
      if(rec2._4 <= rec1._4 && rec2._4 >= rec1._3)
        rec_distance=util_define.point_dist(rec1._1,rec2._4,rec2._2,rec2._4)
    }
    if(rec2._1 <= rec1._1 && rec2._1 >= rec1._2)
    {
      if(rec2._3>rec1._4)          //right below
        rec_distance=util_define.point_dist(rec2._1,rec1._4,rec2._1,rec2._3)
      else if(rec2._4 < rec1._3)             //left below
        rec_distance=util_define.point_dist(rec2._1,rec1._3,rec2._1,rec2._4)
      else
        rec_distance=0.0
    }
    if(rec2._2 <= rec1._1 && rec2._2 >= rec1._2)
    {
      if(rec2._3 > rec1._4)                   //rigth upper
        rec_distance=util_define.point_dist(rec2._2,rec1._4,rec2._2,rec2._3)
      else if(rec2._4 < rec1._3)               //left upper
        rec_distance=util_define.point_dist(rec2._2,rec1._3,rec2._2,rec2._4)
      else
        rec_distance=0.0
    }
    rec_distance
  }

}
