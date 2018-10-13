package org.lasersonlab.zarr

import lasersonlab.{ zarr ⇒ z }
import org.hammerlab.paths.Path
import org.lasersonlab.zarr
import org.lasersonlab.zarr.cmp.Cmp
import org.lasersonlab.zarr.dtype.DataType
import org.lasersonlab.zarr.io.Load
import org.lasersonlab.zarr.utils.Idx

class GroupTest
  extends hammerlab.test.Suite
     with HasGetOps
     with Load.syntax
     with zarr.cmp.all
     with Cmp.syntax
     with cmp.path {

  implicit val __int: Idx.T[Int] = Idx.Int

  import DataType.{ untyped ⇒ _, _ }

  val bytes =
    for {
      x ← 1 to  50
      y ← 1 to 100
      z ← 1 to 200
    } yield (
      if (y % 2 == 0) '0' else
      if (z % 3 == 0) 'A' else
      if (x % 2 == 0) 'z' else
                      ' '
    )
    .toByte

  val  shorts = (1  to 10).map(_.toShort)
  val    ints =  1  to 10
  val   longs =  1L to 10L
  val  floats =  0f until 10f  by 0.5f
  val doubles = 0.0 until 10.0 by 0.5
  test("typed") {
    import lasersonlab.shapeless.slist.{ == ⇒ _, _ }
    import GroupTest.{ == ⇒ _, _ }

    val group =
      Foo(
          bytes = Array(50 :: 100 :: 200 :: ⊥, 20 :: 50 :: 110 :: ⊥)(   bytes: _* ),
         shorts = Array(              10 :: ⊥                      )(  shorts: _* ),
           ints = Array(              10 :: ⊥                      )(    ints: _* ),
          longs = Array(              10 :: ⊥                      )(   longs: _* ),
         floats = Array(              20 :: ⊥                      )(  floats: _* ),
        doubles = Array(              20 :: ⊥                      )( doubles: _* ),
        strings =
          Strings(
            s2 = {
              implicit val d = string(2)
              Array(10 :: ⊥)((1 to 10).map(_.toString): _*)
            },
            s3 = {
              implicit val d = string(3)
              Array(
                10 :: 10 :: ⊥,
                 3 ::  4 :: ⊥
              )(
                (1 to 100).map(_.toString): _*
              )
            }
          ),
        structs =
          Structs(
            ints =
              Array(10 :: ⊥)(
                (1 to 10).map { I4 }: _*
              ),
            numbers =
              Array(
                10 :: 10 :: ⊥,
                 2 ::  5 :: ⊥
              )(
                (
                  for {
                    r ← 0 until 10
                    c ← 0 until 10
                    n = 10 * r + c
                  } yield
                    Numbers(
                       short = n.toShort,
                         int = n,
                        long = n          * 1e9 toLong,
                       float = n.toFloat  *  10,
                      double = n.toDouble * 100
                    )
                )
                : _*
              )
          )
      )

    val actual = tmpDir()

    group.save(actual).!

    val expectedPath = resource("grouptest.zarr")

    eqv(actual, expectedPath)

    val group2 = actual.load[Foo] !

    eqv(group, group2)

    val expected = expectedPath.load[Foo] !

    eqv(group, expected)
  }

  test("untyped") {
    val group =
      Group(
        arrays =
          // TODO: remove need for explicit types on this Map
          Map[String, Array.*?[Int]](
              "bytes" → Array(50 :: 100 :: 200 :: Nil, 20 :: 50 :: 110 :: Nil)(   bytes: _* ),
             "shorts" → Array(              10 :: Nil                        )(  shorts: _* ),
               "ints" → Array(              10 :: Nil                        )(    ints: _* ),
              "longs" → Array(              10 :: Nil                        )(   longs: _* ),
             "floats" → Array(              20 :: Nil                        )(  floats: _* ),
            "doubles" → Array(              20 :: Nil                        )( doubles: _* )
          ),
        groups =
          Map(
            "strings" →
              Group(
                Map[String, Array.*?[Int]](
                  "s2" → {
                    implicit val d = string(2)
                    Array(10 :: Nil)((1 to 10).map(_.toString): _*)
                  },
                  "s3" → {
                    implicit val d = string(3)
                    Array(
                      10 :: 10 :: Nil,
                       3 ::  4 :: Nil
                    )(
                      (1 to 100).map(_.toString): _*
                    )
                  }
                )
              ),
            "structs" →
              Group(
                Map[String, Array.*?[Int]](
                  "ints" → {
                    implicit val datatype =
                      DataType.untyped.Struct(
                        StructEntry("value", int) :: Nil
                      )
                    Array(10 :: Nil)(
                      (1 to 10)
                        .map {
                          i ⇒
                            untyped.Struct("value" → i)
                        }
                      : _*
                    )
                  },
                  "numbers" → {
                    implicit val datatype =
                      DataType.untyped.Struct(
                        List(
                          StructEntry( "short",  short),
                          StructEntry(   "int",    int),
                          StructEntry(  "long",   long),
                          StructEntry( "float",  float),
                          StructEntry("double", double)
                        )
                      )
                    Array(
                      10 :: 10 :: Nil,
                       2 ::  5 :: Nil
                    )(
                      (
                        for {
                          r ← 0 until 10
                          c ← 0 until 10
                          n = 10 * r + c
                        } yield
                          untyped.Struct(
                             "short" →  n.toShort,
                               "int" →  n,
                              "long" → (n          * 1e9 toLong),
                             "float" → (n.toFloat  *  10),
                            "double" → (n.toDouble * 100)
                          )
                      )
                      : _*
                    )
                  }
                )
              )
          )
      )

    val actual = tmpDir()
    group.save(actual).!

    val group2 = actual.load[Group[Int]] !

    eqv(group, group2)

    val expected = resource("grouptest.zarr").load[Group[Int]] !

    eqv(group, expected)
  }
}

object GroupTest {
  case class I4(value: Int)
  case class Numbers(
     short: Short,
       int: Int,
      long: Long,
     float: Float,
    double: Double
  )

  import lasersonlab.shapeless.slist._

  case class Foo(
      bytes: z.Array[`3`,   Byte],
     shorts: z.Array[`1`,  Short],
       ints: z.Array[`1`,    Int],
      longs: z.Array[`1`,   Long],
     floats: z.Array[`1`,  Float],
    doubles: z.Array[`1`, Double],
    strings: Strings,
    structs: Structs
  )

  case class Strings(
    s2: z.Array[`1`, String],
    s3: z.Array[`2`, String]
  )

  case class Structs(
       ints: z.Array[`1`,      I4],
    numbers: z.Array[`2`, Numbers]
  )
}
