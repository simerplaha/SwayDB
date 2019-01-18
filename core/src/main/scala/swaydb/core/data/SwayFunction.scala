package swaydb.core.data

import scala.concurrent.duration.Deadline
import swaydb.data.slice.Slice

sealed trait SwayFunction

object SwayFunction {
  sealed trait RequiresKey extends SwayFunction
  sealed trait RequiresValue extends SwayFunction
  sealed trait RequiresDeadline extends SwayFunction

  case class Key(f: Slice[Byte] => SwayFunctionOutput) extends RequiresKey
  case class KeyDeadline(f: (Slice[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresDeadline
  case class KeyValue(f: (Slice[Byte], Option[Slice[Byte]]) => SwayFunctionOutput) extends RequiresKey with RequiresValue

  case class KeyValueDeadline(f: (Slice[Byte], Option[Slice[Byte]], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresValue  with RequiresDeadline
  case class Value(f: Option[Slice[Byte]] => SwayFunctionOutput) extends RequiresValue
  case class ValueDeadline(f: (Option[Slice[Byte]], Option[Deadline]) => SwayFunctionOutput) extends RequiresValue  with RequiresDeadline
}

sealed trait SwayFunctionOutput {
  def toValue(time: Time): Value.RangeValue
}
object SwayFunctionOutput {

  case object Remove extends SwayFunctionOutput {
    def toValue(time: Time): Value.Remove =
      Value.Remove(None, time)
  }

  case class Expire(deadline: Deadline) extends SwayFunctionOutput {
    def toValue(time: Time): Value.Remove =
      Value.Remove(Some(deadline), time)
  }

  case class Update(value: Option[Slice[Byte]], deadline: Option[Deadline]) extends SwayFunctionOutput {
    def toValue(time: Time): Value.Update =
      Value.Update(value, deadline, time)
  }
}
