package swaydb.core.segment.merge

import scala.util.Failure
import swaydb.data.slice.Slice

object MergeException {

  val keyNotProvidedMergingIntoPutFailure = Failure(KeyNotProvidedMergingIntoPut)

  case class FunctionNotFound(name: Slice[Byte]) extends Exception(s"Function '${name.readString()}' not found.")
  case object KeyNotProvidedMergingIntoPut extends Exception("Key not provided merging into put.")

}
