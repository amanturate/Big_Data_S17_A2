import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object Assignment_17_2 extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .master("local")
    .appName("example")
    .getOrCreate()

  val sc = spark.sparkContext

  //Loading the text file
  val csvDF1 = sc.textFile("C:/ACADGILD/Big Data/SESSION_17/17.2_Dataset.txt")

  //We are creating a Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
  // partitioned collection of elements that can be operated on in parallel.

  val arrayTuples = csvDF1.map(line => line.split(","))
                          .map(array => (array(0), array(1), array(2), array(3), array(4)))
                          .collect
  println("Total number of rows = " + arrayTuples.length)
  println("******************************************************************************************")
  //For geting distinct subjects we will use distinct function which returns distinct values of field.
  val distinctRdd = arrayTuples.map(x => x._2).distinct.toList

  println("Distinct subjects are :\n" + distinctRdd.mkString("\n"))
  println("******************************************************************************************")
  println("Distinct number of subjects present in the entire school = "+(distinctRdd).length)
  println("******************************************************************************************")

  //For selecting specific value we will use filter statement
  //which returns the tuple with matching values
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
  println("Average of each student in each grade = " + sum_b.mkString("\n") )
  println("******************************************************************************************")

  //Grouping on the basis of key - In groupby, the RDD is reduced on the basis of key provided
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

  val gp_ag = sum_a.map(x => (x(0),x(3).toFloat)).groupByKey().mapValues(x => x.sum/x.size).collect()
  val gp_pg = sum_a.map(x => ((x(0),x(2)),x(3).toFloat)).groupByKey().mapValues(x => x.sum/x.size).collect()
  val gp = gp_pg.map(x=>(x._1._1,x._2))

  //Intersection is a function which provides the comoon values between two rdd
  val gp_pg_1 = gp_ag.intersect(gp)
  println("Average score per student_name across all grades " + gp_ag.mkString("\n"))
  println("average score per student_name per grade " + gp_pg.mkString("\n"))
  println("Number of students who satisfy the condition : " + gp_pg_1.length)
}
