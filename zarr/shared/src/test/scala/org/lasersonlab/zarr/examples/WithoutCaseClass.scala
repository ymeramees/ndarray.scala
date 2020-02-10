package org.lasersonlab.zarr.examples


object WithoutCaseClass {
  def main(args: Array[String]): Unit = {

    import lasersonlab.zarr._

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent._
    import scala.concurrent.duration._

    def get[T](f: Future[T]): T = Await.result(f, 10 seconds)

    def createTestFoo = { // If data for foo is not existing yet
      // Schema for a type that we will instatiate and then save as a Zarr "group" (a top-level directory with
      // subdirectories for each "array" field
      case class Foo(
                      ints: Array[`2`, Int], // 2-D array of ints
                      doubles: Array[`3`, Double] // 3-D array of doubles
                    )

      // Instantiate a Foo
      val foo =
        Foo(
          ints = Array(1000 :: 1000 :: ⊥)(1 to 1000000),
          doubles = Array(100 :: 100 :: 100 :: ⊥)((1 to 1000000).map(_.toDouble))
        )

      // Save as a Zarr group: a directory containing "ints" and "doubles" subdirectories, each a Zarr array:
      foo.save(Path("foo"))
    }

    val path = Path("foo")
    if (!get(path.exists))
      createTestFoo
    val zarrGroup = get(path.load[Group])

    println(s"Group keys: ${zarrGroup.groups.keySet}")
    println(s"Array keys: ${zarrGroup.arrays.keySet}")
    
    val ints = zarrGroup.array("ints").t.toList
    println(s"Ints beginning: ${ints.take(100)}")
    println(s"Ints end: ${ints.takeRight(100)}")
  }
}
