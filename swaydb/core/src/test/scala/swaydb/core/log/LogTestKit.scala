package swaydb.core.log

import org.scalatest.matchers.should.Matchers._
import swaydb.{Bag, Glass}
import swaydb.core.log.serialiser.LogEntryWriter
import swaydb.core.segment.data.{KeyValue, Memory}
import swaydb.core.TestExecutionContext
import swaydb.core.file.sweeper.bytebuffer.ByteBufferCommand
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.slice.{Slice, SliceOption}
import swaydb.slice.order.KeyOrder
import swaydb.IOValues._
import swaydb.testkit.RunThis._

import scala.concurrent.duration._

object LogTestKit {

  implicit class SliceKeyValueImplicits(actual: Iterable[KeyValue]) {
    def toLogEntry(implicit serialiser: LogEntryWriter[LogEntry.Put[Slice[Byte], Memory]]) =
    //LevelZero does not write Groups therefore this unzip is required.
      actual.foldLeft(Option.empty[LogEntry[Slice[Byte], Memory]]) {
        case (logEntry, keyValue) =>
          val newEntry = LogEntry.Put[Slice[Byte], Memory](keyValue.key, keyValue.toMemory())
          logEntry.map(_ ++ newEntry) orElse Some(newEntry)
      }
  }

  implicit class LogsImplicit[OK, OV, K <: OK, V <: OV](logs: Logs[K, V, _]) {

    /**
     * Manages closing of Map accouting for Windows where
     * Memory-mapped files require in-memory ByteBuffer be cleared.
     */
    def ensureClose(): Unit = {
      implicit val ec = TestExecutionContext.executionContext
      implicit val bag = Bag.future
      logs.close().value
      logs.bufferCleaner.actor().receiveAllForce[Glass, Unit](_ => ())
      (logs.bufferCleaner.actor() ask ByteBufferCommand.IsTerminated[Unit]).await(10.seconds)
    }
  }

  implicit class LogEntryImplicits(actual: LogEntry[Slice[Byte], Memory]) {

    def shouldBe(expected: LogEntry[Slice[Byte], Memory]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize
      actual.totalByteSize shouldBe expected.totalByteSize
      actual match {
        case LogEntry.Put(key, value) =>
          val exp = expected.asInstanceOf[LogEntry.Put[Slice[Byte], Memory]]
          key shouldBe exp.key
          value shouldBe exp.value

        case LogEntry.Remove(key) =>
          val exp = expected.asInstanceOf[LogEntry.Remove[Slice[Byte]]]
          key shouldBe exp.key

        case batch: LogEntry.Batch[Slice[Byte], Memory] => //LogEntry is a batch of other MapEntries, iterate and assert.
          expected.entries.size shouldBe batch.entries.size
          expected.entries.zip(batch.entries) foreach {
            case (expected, actual) =>
              actual shouldBe expected
          }
      }
    }
  }

  implicit class SegmentsPersistentMapImplicits(actual: LogEntry[Slice[Byte], Segment]) {

    def shouldBe(expected: LogEntry[Slice[Byte], Segment]): Unit = {
      actual.entryBytesSize shouldBe expected.entryBytesSize

      val actualMap = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(KeyOrder.default)
      actual.applyBatch(actualMap)

      val expectedMap = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(KeyOrder.default)
      expected.applyBatch(expectedMap)

      actualMap.size shouldBe expectedMap.size

      val actualArray = actualMap.toIterable.toArray
      val expectedArray = expectedMap.toIterable.toArray

      actualArray.indices.foreach {
        i =>
          val actual = actualArray(i)
          val expected = expectedArray(i)
          actual._1 shouldBe expected._1
          actual._2 shouldBe expected._2
      }
    }
  }

}
