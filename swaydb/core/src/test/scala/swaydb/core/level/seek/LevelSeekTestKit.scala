package swaydb.core.level.seek

import org.scalatest.matchers.should.Matchers._
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.data._
import swaydb.core.segment.data.KeyValueTestKit.unexpiredPuts
import swaydb.core.segment.ref.search.SegmentSearchTestKit.{assertEmptyHeadAndLast, assertHigher, assertLower}
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.effect.IOValues._
import swaydb.slice.Slice

import scala.collection.parallel.CollectionConverters._
import scala.util.Random

object LevelSeekTestKit {

  def assertGet(keyValues: Iterable[KeyValue],
                level: LevelRef): Unit =
    keyValues foreach {
      keyValue =>
        try
          level.get(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut match {
            case Some(got) =>
              got shouldBe keyValue

            case None =>
              unexpiredPuts(Slice(keyValue)) should have size 0
          }
        catch {
          case ex: Throwable =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" expired: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            fail(ex)
        }
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelRef): Unit =
    keyValues foreach {
      keyValue =>
        try
          level.get(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
        catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }

  def assertGetNone(keyValues: Iterable[KeyValue],
                    level: LevelZero): Unit =
    keyValues.par foreach {
      keyValue =>
        level.get(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe None
    }

  def assertGetNone(keys: Range,
                    level: LevelRef): Unit =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key), ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
    }

  def assertGetNone(keys: List[Int],
                    level: LevelRef): Unit =
    keys.par foreach {
      key =>
        level.get(Slice.writeInt(key), ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
    }

  def assertGetNoneButLast(keyValues: Iterable[KeyValue],
                           level: LevelRef): Unit = {
    keyValues.dropRight(1).par foreach {
      keyValue =>
        level.get(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
    }

    keyValues
      .lastOption
      .map(_.key)
      .flatMap(level.get(_, ThreadReadState.random).runRandomIO.get.toOptionPut.map(_.toMemory())) shouldBe keyValues.lastOption
  }

  def assertGetNoneFromThisLevelOnly(keyValues: Iterable[KeyValue],
                                     level: Level): Unit =
    keyValues foreach {
      keyValue =>
        level.getFromThisLevel(keyValue.key, ThreadReadState.random).runRandomIO.get.toOption shouldBe empty
    }

  /**
   * If all key-values are non put key-values then searching higher for each key-value
   * can result in a very long search time. Considering using shuffleTake which
   * randomly selects a batch to assert for None higher.
   */
  def assertHigherNone(keyValues: Iterable[KeyValue],
                       level: LevelRef,
                       shuffleTake: Option[Int] = None): Unit = {
    val unzipedKeyValues = keyValues
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzipedKeyValues).take) getOrElse unzipedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          //          println(keyValue.key.readInt())
          level.higher(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
          //          println
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }
  }

  def assertLowerNone(keyValues: Iterable[KeyValue],
                      level: LevelRef,
                      shuffleTake: Option[Int] = None): Unit = {
    val unzippedKeyValues = keyValues
    val keyValuesToAssert = shuffleTake.map(Random.shuffle(unzippedKeyValues).take) getOrElse unzippedKeyValues
    keyValuesToAssert foreach {
      keyValue =>
        try {
          level.lower(keyValue.key, ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
        } catch {
          case ex: Exception =>
            println(
              "Test failed for key: " + keyValue.key.readInt() +
                s" indexEntryDeadline: ${keyValue.toMemory().indexEntryDeadline.map(_.hasTimeLeft())}" +
                s" class: ${keyValue.getClass.getSimpleName}"
            )
            throw ex
        }
    }
  }

  def assertReads(keyValues: Iterable[KeyValue],
                  level: LevelRef): Unit = {
    val asserts = Seq(() => assertGet(keyValues, level), () => assertHigher(keyValues, level), () => assertLower(keyValues, level))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertNoneReads(keyValues: Iterable[KeyValue],
                      level: LevelRef): Unit = {
    val asserts = Seq(() => assertGetNone(keyValues, level), () => assertHigherNone(keyValues, level), () => assertLowerNone(keyValues, level))
    Random.shuffle(asserts).par.foreach(_ ())
  }

  def assertEmpty(keyValues: Iterable[KeyValue],
                  level: LevelRef): Unit = {
    val asserts =
      Seq(
        () => assertGetNone(keyValues, level),
        () => assertHigherNone(keyValues, level),
        () => assertLowerNone(keyValues, level),
        () => assertEmptyHeadAndLast(level)
      )
    Random.shuffle(asserts).par.foreach(_ ())
  }
}
