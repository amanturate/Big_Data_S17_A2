import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object Assignemtn_17_2 extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .master("local")
    .appName("example")
    .getOrCreate()

  val sc = spark.sparkContext

  //Loading the text file
  val csvDF1 = sc.textFile("C:/ACADGILD/Big Data/SESSION_17/17.2_Dataset.txt")
  val arrayTuples = csvDF1.map(line => line.split(","))
                          .map(array => (array(0), array(1), array(2), array(3), array(4)))
                          .collect
  println("Total number of rows = " + arrayTuples.length)
  println("******************************************************************************************")
  val distinctRdd = arrayTuples.map(x => x._2).distinct.toList
  println("Distinct subjects are :\n" + distinctRdd.mkString("\n"))
  println("******************************************************************************************")
  println("Distinct number of subjects present in the entire school = "+(distinctRdd).length)
  println("******************************************************************************************")
  val distinctdata = arrayTuples.filter(x=>x._1=="Mathew" && x._4 == "55")

  println("The count of the number of students in the school, whose name is Mathew and marks is 55 = "
            + distinctdata.length)
  println("###########################################################################################")

  val stud_per_grd =  csvDF1.map(_.split(",")).map(_(2)).countByValue()
  println("the count of students per grade in the school :\n" + stud_per_grd.mkString("\n"))
  println("******************************************************************************************")
  val sum_a = csvDF1.map(x=>x.split(","))


  val sum_b = sum_a.map(line=>((line(0),line(2)),line(3).toFloat))
                    .groupByKey().mapValues(x => x.sum/x.size).collect()
  println("Average of each student = " + sum_b.mkString("\n") )
  println("******************************************************************************************")

  val sum_c = sum_a.map(line =>(line(1),line(3).toFloat))
                    .groupByKey().mapValues(x => x.sum/x.size).collect()

  println("The average score of students in each subject across all grades = " + sum_c.mkString("\n") )
  println("******************************************************************************************")

  val sum_d = sum_a.map(line =>((line(1),line(2)),line(3).toFloat))
    .groupByKey().mapValues(x => x.sum/x.size).collect()

  println("the average score of students in each subject per grade = " + sum_d.mkString("\n") )
  println("******************************************************************************************")

  val sum_e = sum_a.filter(x=>x(2)=="grade-2").map(line => (line(0),line(3).toFloat))
                      .groupByKey().mapValues(x => x.sum/x.size)

  val sum_ee = sum_e.filter(_._2 > 50.00).collect()
  println("students in grade-2, having average score greater than 50 :" + sum_ee.mkString("\n"))
  println("******************************************************************************************")
}
