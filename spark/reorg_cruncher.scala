import org.apache.spark.sql.types._

def reorg(datadir :String) 
{
  val t0 = System.nanoTime()

  // nothing here (yet)

  val t1 = System.nanoTime()
  println("reorg time: " + (t1 - t0)/1000000 + "ms")
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
  val t0 = System.nanoTime()

  // load the three tables
  val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/person.*.csv.*")
  val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/interest.*.csv.*")
  val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/knows.*.csv.*")

  // select the relevant (personId, interest) tuples, and add a boolean column "nofan" (true iff this is not a a1 tuple)
  val p1_cands = interest
                  .filter($"interest" === a1)
                  .join(person, "personId")
                  .withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday"))
                  .filter($"bday" >= lo && $"bday" <= hi)
                  .select($"personId".alias("person1Id"), $"locatedIn".alias("person1City"))

  // compute scores of person2/3: not (yet) a fan of a1 and #relevant interests from a2, a3, a4
  // join, groupby & aggregate
  val p23_scores = interest               
                 .filter($"interest" isin (a1, a2, a3, a4))
                 .withColumn("nofan", $"interest".notEqual(a1))
                 .join(person, "personId")
                 .groupBy("personId", "locatedIn")
                 .agg(count("personId") as "score", min("nofan") as "nofan")

  // filter (personId, score, locatedIn) tuples with score >= 2, not a fan of a1
  val p23_cands = p23_scores.filter($"score" >= 2 && $"nofan").
                    select(
                      $"personId".alias("person23Id"),
                      $"score".alias("person23Score"),
                      $"locatedIn".alias("person23City")
                    )

  // person2/3, its score/city and the friend id
  val knows_cands = knows.join(p23_cands, knows("personId") === p23_cands("person23Id")).
                      select($"person23Id", $"person23Score", $"person23City", $"friendId")
  
  // p12
  val p12 =
    p1_cands.join(knows_cands, p1_cands("person1Id") === knows_cands("friendId")
                           && p1_cands("person1City") === knows_cands("person23City"))
           .select($"person1Id", $"person1City", $"person23Id".alias("person2Id"), $"person23Score".alias("person2Score"))

  val p123 =
    p12.join(knows_cands, p12("person1Id") === knows_cands("friendId")
                       && p12("person1City") === knows_cands("person23City")
                       && p12("person2Id") < knows_cands("person23Id"))
           .select(p12("person1Id"), $"person2Id", $"person2Score", $"person23Id".alias("person3Id"), $"person23Score".alias("person3Score"))

  val p123triangle =
     p123.join(knows_cands, p123("person2Id") === knows_cands("friendId")
                         && p123("person3Id") === knows_cands("person23Id"))
         .select($"person1Id", $"person2Id", $"person3Id", ($"person2Score" + $"person3Score").alias("score"))

  // sort the result
  val ret = p123triangle.orderBy(desc("score"), asc("person1Id"), asc("person2Id"), asc("person3Id"))

  ret.show(1000) // force execution now, and display results to stdout

  val t1 = System.nanoTime()
  println("cruncher time: " + (t1 - t0)/1000000 + "ms")

  return ret
}
