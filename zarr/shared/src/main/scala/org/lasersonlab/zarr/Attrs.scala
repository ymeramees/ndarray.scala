package org.lasersonlab.zarr

import io.circe.{ Decoder, Encoder }
import org.lasersonlab.zarr.group.Basename

case class Attrs(json: io.circe.Json)

object Attrs {
  val basename = ".zattrs"
  implicit val _basename = Basename[Attrs](basename)

  implicit val encoder: Encoder[Attrs] = Encoder.instance(_.json)
  implicit val decoder: Decoder[Attrs] = Decoder.instance(c ⇒ Right(Attrs(c.value)))
}
