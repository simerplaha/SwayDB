package swaydb.core.data

import scala.util.Success
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object Time {

  val empty = Time(Slice.emptyBytes)
  val someEmpty = Some(empty)
  val successEmpty = Success(empty)

  def >(upperTime: Time, lowerTime: Time)(implicit timeOrder: TimeOrder[Slice[Byte]]): Boolean = {
    import timeOrder._
    if (upperTime.nonEmpty && lowerTime.nonEmpty)
      upperTime.time > lowerTime.time
    else
      true //if timeOne and timeTwo are the same this means the upper is already merged into lower so lower is greater.
  }

  implicit class TimeOptionImplicits(time: Time) {
    def >(otherTime: Time)(implicit timeOrder: TimeOrder[Slice[Byte]]): Boolean =
      Time > (time, otherTime)
  }

  def fromApplies(applies: Slice[Value.Apply]): Time =
    applies
      .reverse
      .find(_.time.nonEmpty)
      .map(_.time)
      .getOrElse(Time.empty)
}

case class Time(time: Slice[Byte]) {
  def unslice(): Time =
    Time(time.unslice())

  def isEmpty =
    time.isEmpty

  def nonEmpty =
    !isEmpty

  def size =
    time.size
}
