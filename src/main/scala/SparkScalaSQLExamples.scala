import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

case class Author(name: String, age: Int)

object SparkScalaSQLExamples extends App{
  val sc = new SparkContext("local", "App")
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)

  val sql =
    """
      |select distinct name from
    """.stripMargin

  //implicit schema

  val books = sc.parallelize(List(Author("Dickens",200),
    Author("Leckie",45),
    Author("Stoker",300)))
  import sqlContext.implicits._

  val booksDf = books.toDF("name", "totalBooks")
  booksDf.printSchema()
  booksDf.show()
  booksDf.registerTempTable("ClassAuthors")
  val classAuthors = sqlContext.sql(sql + "ClassAuthors")
  classAuthors.show()


  //explicit schema
  val booksTuple: RDD[Row] = sc.parallelize(List(("Dickens",30),
    ("Ann Leckie",3),
    ("Bram Stoker",10),
    ("Shakey Stevens",3),
    ("John Smith",10)))
  .map { case(name, count) => Row(name, count)}
  val schema = StructType(List(
    StructField("name", StringType, false),
    StructField("count", IntegerType, false)
  ))
  val df = sqlContext.createDataFrame(booksTuple, schema)
  df.printSchema()
  df.registerTempTable("authors")

  val authors = sqlContext.sql(sql + "authors")
  authors.show()

  val join = sqlContext.sql(
    """
      |select a.*
      |      ,ca.totalBooks
      |from ClassAuthors ca
      |full outer join
      |     authors a
      |on ca.name = a.name
    """.stripMargin)
  join.show()


  //val joinFiltered = join.select("name").where("totalBooks = 45")
  //joinFiltered.show()

  //UDF
  def initials(args: String) = {
    args.split(" ").map(_.substring(0,1)).mkString("")
  }
  sqlContext.udf.register("initials", (s: String) => initials(s))
  sqlContext.sql("select name, initials(name) as initials from authors").show()
  //df.select("name",initials("name")).show()


}
