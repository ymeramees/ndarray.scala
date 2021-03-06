package org.lasersonlab.singlecell.hdf5

import com.tom_e_white.hdf5_java_cloud.{ ArrayUtils, NioReadOnlyRandomAccessFile }
import hammerlab.path._
import org.apache.spark.mllib.linalg.{ SparseVector, Vector, Vectors }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row, SaveMode, SparkSession }
import ucar.nc2.NetcdfFile

object Parquet {

  def apply(input: Path, partitions: Int, output: Path)(implicit spark: SparkSession): Unit = {
    val t0 = System.currentTimeMillis()

//    val file = "files/1M_neurons_filtered_gene_bc_matrices_h5.h5" // change to "gs://..." for GCS
//    val output = "10x_parquet"

    val sc = spark.sparkContext

    // Read the k'th shard of the HDF5 file and return a sequence of barcode-vector tuples. Each shard must fit in memory.
    def readShard(k: Int): Seq[(String, Vector)] = {
      val raf = new NioReadOnlyRandomAccessFile(input)
      val ncfile = NetcdfFile.open(raf, input.toString, null, null)
      val indptr = ncfile.findVariable("/mm10/indptr")
      val indices = ncfile.findVariable("/mm10/indices")
      val data = ncfile.findVariable("/mm10/data")
      val barcodes = ncfile.findVariable("/mm10/barcodes")
      val shape = ncfile.findVariable("/mm10/shape")

      val numFeatures = shape.read.getInt(0)

      val numRows = barcodes.getShape(0)
      val start = k * numRows / (partitions - 1)
      var end = 0
      if (k == (partitions - 1)) end = numRows
      else end = (k + 1) * numRows / (partitions - 1)

      val barcodeData: Array[String] = ArrayUtils.index(barcodes, start, end + 1)
                                       .copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
        .map(x => x.mkString)
      val indptrData: Array[Long] = ArrayUtils.index(indptr, start, end + 1).getStorage.asInstanceOf[Array[Long]]
      val firstIndptr: Long = indptrData(0)
      val lastIndptr: Long = indptrData.last
      if (firstIndptr == lastIndptr) {
        return Seq()
      }
      val indicesData: Array[Long] = ArrayUtils.index(indices, firstIndptr, lastIndptr).getStorage.asInstanceOf[Array[Long]]
      val dataData: Array[Int] = ArrayUtils.index(data, firstIndptr, lastIndptr).getStorage.asInstanceOf[Array[Int]]

      (0 until end - start).map {
        i ⇒
          val barcode = barcodeData(i)
          val indicesSlice = indicesData.slice((indptrData(i) - firstIndptr).toInt, (indptrData(i + 1) - firstIndptr).toInt)
          val dataSlice = dataData.slice((indptrData(i) - firstIndptr).toInt, (indptrData(i + 1) - firstIndptr).toInt)
          val indexDataPairs =
            indicesSlice
            .zip(dataSlice)
            .map {
              case (k: Long, v: Int) ⇒
                (k.toInt, v.toDouble)  // Vector is (Int, Double)
            }
          val vec = Vectors.sparse(numFeatures, indexDataPairs)
          (barcode, vec)
      }
    }

    val actualShards = partitions  // change this to test on a subset
    val shardIndexes = sc.parallelize(0 until actualShards, partitions)
    val rows =
      shardIndexes
      .flatMap(readShard)
      .map {
        case (id, vec) ⇒
          Row(
            id,
            vec
            .asInstanceOf[SparseVector]
              .indices,
            vec
            .asInstanceOf[SparseVector]
              .values
          )
      }

    val schema = StructType(
      StructField(   "id", StringType                    , false) ::
      StructField(  "idx",  ArrayType(IntegerType, false), false) ::
      StructField("quant",  ArrayType( DoubleType, false), false) :: Nil
    )

    val df = spark.createDataFrame(rows, schema)
    df.write.mode(SaveMode.Overwrite).parquet(output.toString)

    val t1 = System.currentTimeMillis()

    println("Elapsed time: " + ((t1 - t0) / 1000) + "s")
  }
}
