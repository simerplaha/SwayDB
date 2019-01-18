package swaydb.core

import java.util.concurrent.atomic.AtomicLong
import scala.util.Random
import swaydb.core.data.Time
import swaydb.data.slice.Slice
import swaydb.macros.SealedList
import swaydb.serializers.Default.LongSerializer
import swaydb.serializers._

sealed trait TestTimeGenerator {
  def nextTime: Time

  def startTime: Long
}

object TestTimeGenerator {

  case class Incremental(startTime: Long = 0) extends TestTimeGenerator {
    val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      Time(timer.incrementAndGet())
  }

  object IncrementalRandom extends TestTimeGenerator {
    override val startTime: Long = 0
    private val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      if (Random.nextBoolean())
        Time(timer.incrementAndGet())
      else
        Time.empty

  }

  case class Decremental(startTime: Long = 100) extends TestTimeGenerator {
    val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      Time(timer.decrementAndGet())
  }

  object DecrementalRandom extends TestTimeGenerator {
    override val startTime: Long = 100
    private val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      if (Random.nextBoolean())
        Time(timer.decrementAndGet())
      else
        Time.empty
  }

  object Empty extends TestTimeGenerator {
    override val startTime: Long = 0
    override val nextTime: Time =
      Time(Slice.emptyBytes)
  }

  val all =
    SealedList.list[TestTimeGenerator]

  def random: TestTimeGenerator =
    Random.shuffle(all).head

}
