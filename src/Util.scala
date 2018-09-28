import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.FSDataInputStream
import java.io.InputStreamReader
import java.io.BufferedReader

case class Point_id(lng:Double,lat:Double)
case class Order(tInd:String,fInd:String,time1:Int,time2:Int,num_ord:Int)
case class Order_2(tInd:String,fInd:String,time1:Int,time2:Int,num_ord:Int,mod:Int)
case class Courier(Cid:String)
case class Record(Cid:String,Oid:String)

class Util extends Serializable
{
  def readPoint(path:String)=
  {
    /*val source = Source.fromFile(path)
    var rows = ArrayBuffer[Array[String]]()
    var line = ""
    for (line <- source.getLines)
      rows += line.split(",").map(_.trim)*/
	var rows = ArrayBuffer[Array[String]]()
    var data:Array[(String,Point_id)]=Array()
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
    rows.foreach{x => data = data :+ (x(0),Point_id(x(1).toDouble,x(2).toDouble))}
    data
  }
   
  def readOrder(path:String)=                                                    //电商包裹
  {
    /*val source = Source.fromFile(path)
    var rows = ArrayBuffer[Array[String]]()
    var line = ""
    for (line <- source.getLines)
      rows += line.split(",").map(_.trim)
    var data:Array[(String,Order)]=Array()*/
    var rows = ArrayBuffer[Array[String]]()
	var data:Array[(String,Order)]=Array()
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
    rows.foreach{x => data = data :+ (x(0),Order(x(1),x(2),getMin("08:00"),getMin("20:00"),x(3).toInt))}
    data
  }
 
  def readOrder2(path:String)=                                                       //O2O包裹
  {
    /*val source = Source.fromFile(path)
    var rows = ArrayBuffer[Array[String]]()
    var line = ""
    for (line <- source.getLines)
      rows += line.split(",").map(_.trim)
    var data:Array[(String,Order_2)]=Array()*/
    var rows = ArrayBuffer[Array[String]]()
	var data:Array[(String,Order_2)]=Array()
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
    rows.foreach{x => data = data :+ (x(0),Order_2(x(1),x(2),getMin(x(3)),getMin(x(4)),x(5).toInt,2))}
    data
  }

  def readCourier(path:String)=                                                             //配送员
  {
    /*val source = Source.fromFile(path)
    var rows = ArrayBuffer[Array[String]]()
    var line = ""
    for (line <- source.getLines)
      rows += line.split(",").map(_.trim)
    var data:Array[Courier]=Array()*/var rows = ArrayBuffer[Array[String]]()
	var data:Array[Courier]=Array()
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
    rows.foreach{x => data = data :+ (Courier(x(0)))}
    data = data.filter(value => value.Cid!="Courier_id")
    data
  }
   
  def readRecord(path:String)=                                                //读取plan的记录
  {
    /*val source = Source.fromFile(path)
    var rows = ArrayBuffer[Array[String]]()
    var line = ""
    for (line <- source.getLines)
      rows += line.split(",").map(_.trim)
    var data:Array[Record]=Array()*/
    var rows = ArrayBuffer[Array[String]]()
	var data:Array[(Int,Record)]=Array()
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
	var i =0
	for (i <- 0 to rows.length-1)
        data = data :+ (i,Record(rows(i)(0),rows(i)(1)))
    data
  }
  
  def read_data()=
  {
    val site_points=readPoint("hdfs://master:9000/user/liuyi/logistics/data/1.csv")
    val spot_points=readPoint("hdfs://master:9000/user/liuyi/logistics/data/2.csv")
    val shop_points=readPoint("hdfs://master:9000/user/liuyi/logistics/data/3.csv")
    (site_points,spot_points,shop_points)
  }

  def getMin(time_str:String):Int=                         //time_str???8:00:00之间的分钟差
  {
    var str=new Array[String](2)
    str=time_str.split(':')
    val hh=str(0)
    val mi=str(1)
    return (hh.toInt-"08".toInt)*60+mi.toInt-"00".toInt
  }

  def dist(spot1:String,spot2:String,points_set:Map[String,Point_id]):Int=            //两个地址之间的距???
  { 
    var lat1,lat2,lng1,lng2=200.0               //不存在的经纬度???
    var id1=""
    var id2=""
    
    lng1=points_set(spot1).lng
    lat1=points_set(spot1).lat
    lng2=points_set(spot2).lng
    lat2=points_set(spot2).lat

    if(lat1==200.0 ||lng1==200.0 ||lat2==200.0 || lng2==200.0)
    {
    println(id1+" "+id2+" "+lat1+" "+lat2+" error")
      return -1
    }
    else
    {
      val d= point_dist(lat1,lng1,lat2,lng2)
      return d
    }
  }
 
  def point_dist(lat1:Double,lng1:Double,lat2:Double,lng2:Double):Int=                 //计算两个点之间的距离
  {
    val delta_lat=(lat1-lat2)/2
    val delta_lng=(lng1-lng2)/2
    val tmp=Math.pow(Math.sin(Math.toRadians(delta_lat)),2)+Math.cos(Math.toRadians(lat1))*Math.cos(Math.toRadians(lat2))*Math.pow(Math.sin(Math.toRadians(delta_lng)),2)
    val d= Math.round(2*Math.asin(Math.sqrt(tmp))*6378137/(15*1000/60)).toInt
    return d
  }

    def serv(num_ord:Int):Int=                                //服务时间
  {
    if(num_ord >0)
      return (3*Math.sqrt(num_ord)+5).toInt
    else
      return 0
  }

  def get_flist(site_points:Map[String,Point_id],points:Map[String,Point_id]):Map[String,Array[String]]=
  {
    var F_list:Map[String,Array[String]]=Map()
    var i=0; var maxvalue=0
    site_points.keys.foreach{site_key1=>
      var candidate_list:Array[(Int,String)]=Array()
      site_points.keys.foreach{site_key2=>
        var distance=dist(site_key1,site_key2,points)
        candidate_list=candidate_list:+(distance,site_key2)}      
      candidate_list=candidate_list.sortWith{(a,b) => (a._1 < b._1)}.filter{x=>x._2 != site_key1}
      var flist:Array[String]=Array()
      for(i <- 0 to 4)
        flist=flist:+candidate_list(i)._2
      F_list+=(site_key1->flist)
    }
    return F_list
  }

  def get_elist(shop_points:Map[String,Point_id],site_points:Map[String,Point_id],points:Map[String,Point_id]):Map[String,String]=
  {
    var E_list:Map[String,String]=Map()
    var max_distance=1000
    var nearest=""
    var i=0; var j=0
    shop_points.keys.foreach{shop_key=>
      site_points.keys.foreach{site_key=>
        var distij=dist(shop_key,site_key,points)
        if (max_distance>distij)
        {
          max_distance=distij
          nearest=site_key
        }}
      E_list+=(shop_key->nearest)
    }
    return E_list
  }

}
