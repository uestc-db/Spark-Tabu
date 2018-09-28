
case class Vector(total_cost:Double,max_customer_num:Int,median_customer_num:Int,min_customer_num:Int,total_path:Int,ave_length:Int,ave_arc_length:Int,ave_cost:Double)

class Solution_Vector() extends Serializable
{
  def combine_solution_vector(v1:Array[(Int,Vector)], v2:Array[(Int,Vector)]):Array[(Int,Vector)]=
  {
    var combined:Array[(Int,Vector)]=Array()
    var i = 0
    for (i <- 0 to v1.length-1)
    {
        val total_cost = v1(i)._2.total_cost +v2(i)._2.total_cost
        val max_customer_num = Array(v1(i)._2.max_customer_num, v2(i)._2.max_customer_num).max
        val median_customer_num = (v1(i)._2.median_customer_num + v2(i)._2.median_customer_num)/2                 
        val min_customer_num = Array(v1(i)._2.min_customer_num, v2(i)._2.min_customer_num).min
        val total_path = v1(i)._2.total_path +v2(i)._2.total_path
        val ave_length = (v1(i)._2.total_path*v1(i)._2.ave_length + v2(i)._2.total_path*v2(i)._2.ave_length)/total_path
        val ave_arc_length = (v1(i)._2.ave_arc_length + v2(i)._2.ave_arc_length)/2                        
        val ave_cost = (v1(i)._2.total_path*v1(i)._2.ave_cost + v2(i)._2.total_path*v2(i)._2.ave_cost)/total_path

        var iter = 0
        if (v1(i)._1 == v2(i)._1)
          iter = v1(i)._1
        else
          iter=(-1)
        combined = combined :+ (iter,Vector(total_cost,max_customer_num,median_customer_num,min_customer_num,total_path,ave_length,ave_arc_length,ave_cost))
    }
    return combined
  }
}
