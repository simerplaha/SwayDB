package swaydb.testkit

import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Deadline, DurationInt, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.Random

object TestKit {

  implicit class CommonAssertions[A](instance: A) {
    def shouldBeInstanceOf[B <: A](implicit bClassTag: ClassTag[B]): B = {
      if (!instance.isInstanceOf[B])
      //shouldBe again for descriptive ScalaTest errors
        instance.getClass shouldBe bClassTag.runtimeClass

      instance.asInstanceOf[B]
    }
  }

  def randomByte(): Byte =
    (Random.nextInt(256) - 128).toByte

  def ints(numbers: Int): Int =
    (1 to numbers).foldLeft("") {
      case (concat, _) =>
        concat + Math.abs(Random.nextInt(9)).toString
    }.toInt

  def randomInt(minus: Int = 0): Int =
    Math.abs(Random.nextInt(Int.MaxValue)) - minus - 1

  def randomIntMax(max: Int = Int.MaxValue): Int =
    Math.abs(Random.nextInt(max))

  def randomIntMin(min: Int): Int =
    Math.abs(randomIntMax()) max min

  def randomIntMaxOption(max: Int = Int.MaxValue): Option[Int] =
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

  def randomString(): String =
    randomCharacters()

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

  def randomCharacters(size: Int = 10): String =
    Random.alphanumeric.take(size max 1).mkString

  def genBytes(size: Int = 10): Array[Byte] =
    Array.fill(size)(randomByte())

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

  def anyOrder[T](one: => T, two: => T): List[T] =
    Random.shuffle(List(() => one, () => two)).map(_ ())

  def anyOrder[T](one: => T, two: => T, three: => T): List[T] =
    Random.shuffle(List(() => one, () => two, () => three)).map(_ ())

  def anyOrder[T](one: => T, two: => T, three: => T, four: => T): List[T] =
    Random.shuffle(List(() => one, () => two, () => three, () => four)).map(_ ())

  def anyOrder[T](one: => T, two: => T, three: T, four: => T, five: => T): List[T] =
    Random.shuffle(List(() => one, () => two, () => three, () => four, () => five)).map(_ ())

  def anyOrder[T](one: => T, two: => T, three: T, four: => T, five: => T, six: => T): List[T] =
    Random.shuffle(List(() => one, () => two, () => three, () => four, () => five, () => six)).map(_ ())

  def maybe[T](left: => T): Option[T] =
    if (randomBoolean())
      Some(left)
    else
      None

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
    def toInt: Int =
      if (bool) 1 else 0
  }

  /**
   * Makes it a little easier to run tests on a tuple of same type using iteration.
   */
  case class TestTuple2[T](left: T, right: T) extends Iterable[T] {

    def toTuple: (T, T) =
      (left, right)

    override def iterator: Iterator[T] = Iterator(left, right)
  }

}
