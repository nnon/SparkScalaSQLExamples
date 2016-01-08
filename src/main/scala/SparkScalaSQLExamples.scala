import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

case class Author(name: String, age: Int)

object SparkScalaSQLExamples extends App{
  val sc = new SparkContext("local", "App")
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
    ("Leckie",3),
    ("Stoker",10),
    ("Shakey",3),
    ("Smith",10)))
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
  join.show

}
