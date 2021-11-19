package swaydb.testkit

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Deadline, DurationInt, FiniteDuration}
import scala.util.Random

object TestKit {

  def randomByte() = (Random.nextInt(256) - 128).toByte

  def ints(numbers: Int): Int =
    (1 to numbers).foldLeft("") {
      case (concat, _) =>
        concat + Math.abs(Random.nextInt(9)).toString
    }.toInt

  def randomInt(minus: Int = 0) = Math.abs(Random.nextInt(Int.MaxValue)) - minus - 1

  def randomIntMax(max: Int = Int.MaxValue) =
    Math.abs(Random.nextInt(max))

  def randomIntMin(min: Int) =
    Math.abs(randomIntMax()) max min

  def randomIntMaxOption(max: Int = Int.MaxValue) =
    if (randomBoolean())
      Some(randomIntMax(max))
    else
      None

  def randomNextInt(max: Int): Int =
    Math.abs(Random.nextInt(max))

  def randomBoolean(): Boolean =
    Random.nextBoolean()

  def randomFiniteDuration(maxSeconds: Int = 10): FiniteDuration =
    new FiniteDuration(randomIntMax(maxSeconds), TimeUnit.SECONDS)

  def randomString: String =
    randomCharacters()

  def randomDeadlineOption: Option[Deadline] =
    randomDeadlineOption()

  def randomDeadlineOption(expired: Boolean = randomBoolean()): Option[Deadline] =
    if (randomBoolean())
      Some(randomDeadline(expired))
    else
      None

  def randomDeadline(expired: Boolean = randomBoolean()): Deadline =
    if (expired && randomBoolean())
      0.seconds.fromNow - (randomIntMax(30) + 10).seconds
    else
      (randomIntMax(60) max 30).seconds.fromNow

  def randomCharacters(size: Int = 10) = Random.alphanumeric.take(size max 1).mkString

  def randomBytes(size: Int = 10) = Array.fill(size)(randomByte())

  def randomly[T](f: => T): Option[T] =
    if (Random.nextBoolean())
      Some(f)
    else
      None

  def eitherOne[T](left: => T, right: => T): T =
    if (Random.nextBoolean())
      left
    else
      right

  def someOrNone[T](some: => T): Option[T] =
    if (Random.nextBoolean())
      None
    else
      Some(some)

  def orNone[T](option: => Option[T]): Option[T] =
    if (Random.nextBoolean())
      None
    else
      option

  def anyOrder[T](left: => T, right: => T): Unit =
    if (Random.nextBoolean()) {
      left
      right
    } else {
      right
      left
    }

  def eitherOne[T](left: => T, mid: => T, right: => T): T =
    Random.shuffle(Seq(() => left, () => mid, () => right)).head()

  def eitherOne[T](one: => T, two: => T, three: => T, four: => T): T =
    Random.shuffle(Seq(() => one, () => two, () => three, () => four)).head()

  def eitherOne[T](one: => T, two: => T, three: => T, four: => T, five: => T): T =
    Random.shuffle(Seq(() => one, () => two, () => three, () => four, () => five)).head()

  def eitherOne[T](one: => T, two: => T, three: => T, four: => T, five: => T, six: => T): T =
    Random.shuffle(Seq(() => one, () => two, () => three, () => four, () => five, () => six)).head()

  def expiredDeadline(): Deadline = {
    val deadline = 0.nanosecond.fromNow - 100.millisecond
    assert(!deadline.hasTimeLeft())
    deadline
  }

  def randomExpiredDeadlineOption(): Option[Deadline] =
    if (randomBoolean())
      None
    else
      Some(expiredDeadline())

  def randomBlockSize(): Option[Int] =
    someOrNone(4096)


  implicit class BooleanImplicit(bool: Boolean) {
    def toInt =
      if (bool) 1 else 0
  }
}
