/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.writer

import java.util.concurrent.TimeUnit

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.segment.format.a.entry.reader.DeadlineReader
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class DeadlineReaderWriterSpec extends WordSpec with Matchers {

  val getDeadlineIds =
    allBaseEntryIds collect {
      case entryId: BaseEntryId.GetDeadlineId =>
        entryId
    }

  getDeadlineIds should not be empty

  "applyDeadlineId" should {
    "compress compress deadlines" in {
      getDeadlineIds foreach {
        entryId =>
          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.OneCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(1, entryId) shouldBe true

          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.TwoCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(2, entryId) shouldBe true

          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.ThreeCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(3, entryId) shouldBe true

          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.FourCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(4, entryId) shouldBe true

          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.FiveCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(5, entryId) shouldBe true

          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.SixCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(6, entryId) shouldBe true

          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.SevenCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(7, entryId) shouldBe true

          allBaseEntryIds collect {
            case entryId: BaseEntryId.Deadline.FullyCompressed =>
              entryId
          } contains DeadlineWriter.applyDeadlineId(8, entryId) shouldBe true

          assertThrows[Exception] {
            DeadlineWriter.applyDeadlineId(randomIntMax(100) + 9, entryId) shouldBe true
          }
      }
    }
  }

  "uncompress" should {
    "write deadline as uncompressed" in {
      getDeadlineIds.filter(_.isInstanceOf[BaseEntryId.GetDeadlineId]) foreach { //for all deadline ids
        deadlineID: BaseEntryId.GetDeadlineId =>
          TransientToKeyValueIdBinder.all foreach { //for all key-values
            implicit adjustedEntryId =>
              val deadline = 10.seconds.fromNow
              val deadlineBytes =
                DeadlineWriter.uncompressed(
                  currentDeadline = deadline,
                  getDeadlineId = deadlineID,
                  plusSize = 0,
                  isKeyCompressed = false
                )

              val reader = Reader(deadlineBytes)

              val expectedEntryID = adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineUncompressed.baseId)

              reader.readIntUnsigned().get shouldBe expectedEntryID
              DeadlineReader.DeadlineUncompressedReader.read(reader, None).get should contain(deadline)
          }
      }
    }
  }

  "tryCompress" should {
    "write compressed deadlines with previous deadline" in {
      implicit def bytesToDeadline(bytes: Slice[Byte]): FiniteDuration = FiniteDuration(bytes.readLong(), TimeUnit.NANOSECONDS)

      getDeadlineIds.filter(_.isInstanceOf[BaseEntryId.GetDeadlineId]) foreach { //for all deadline ids
        deadlineID: BaseEntryId.GetDeadlineId =>
          TransientToKeyValueIdBinder.all foreach { //for all key-values
            implicit adjustedEntryId =>

              //Test for when there are zero compressed bytes, compression should return None.
              DeadlineWriter.tryCompress(
                currentDeadline = Deadline(Slice.fill[Byte](8)(0.toByte)),
                previousDeadline = Deadline(Slice.fill[Byte](8)(1.toByte)),
                getDeadlineId = deadlineID,
                plusSize = 0,
                isKeyCompressed = false
              ) shouldBe empty

              //Test for when there are compressed bytes.
              (1 to 8) foreach { //for some or all deadline bytes compressed.
                commonBytes =>
                  val currentDeadlineBytes =
                    Slice.fill[Byte](commonBytes)(0.toByte) ++ Slice.fill[Byte](8 - commonBytes)(1.toByte)

                  val previousDeadlineBytes =
                    Slice.fill[Byte](commonBytes)(0.toByte) ++ Slice.fill[Byte](8 - commonBytes)(2.toByte)

                  val currentDeadline = Deadline(currentDeadlineBytes)
                  val previousDeadline = Deadline(previousDeadlineBytes)

                  val deadlineBytes =
                    DeadlineWriter.tryCompress(
                      currentDeadline = currentDeadline,
                      previousDeadline = previousDeadline,
                      getDeadlineId = deadlineID,
                      plusSize = 0,
                      isKeyCompressed = false
                    )

                  deadlineBytes shouldBe defined

                  val reader = Reader(deadlineBytes.get)

                  val (expectedEntryID, deadlineReader: DeadlineReader[_]) =
                    if (commonBytes == 1)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineOneCompressed.baseId), DeadlineReader.DeadlineOneCompressedReader)
                    else if (commonBytes == 2)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineTwoCompressed.baseId), DeadlineReader.DeadlineTwoCompressedReader)
                    else if (commonBytes == 3)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineThreeCompressed.baseId), DeadlineReader.DeadlineThreeCompressedReader)
                    else if (commonBytes == 4)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineFourCompressed.baseId), DeadlineReader.DeadlineFourCompressedReader)
                    else if (commonBytes == 5)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineFiveCompressed.baseId), DeadlineReader.DeadlineFiveCompressedReader)
                    else if (commonBytes == 6)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineSixCompressed.baseId), DeadlineReader.DeadlineSixCompressedReader)
                    else if (commonBytes == 7)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineSevenCompressed.baseId), DeadlineReader.DeadlineSevenCompressedReader)
                    else if (commonBytes == 8)
                      (adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.deadlineFullyCompressed.baseId), DeadlineReader.DeadlineFullyCompressedReader)

                  reader.readIntUnsigned().get shouldBe expectedEntryID

                  deadlineReader
                    .read(
                      indexReader = reader,
                      previous = Some(randomPutKeyValue(key = 1, deadline = Some(previousDeadline)))
                    ).get should contain(currentDeadline)
              }
          }
      }
    }
  }

  "noDeadline" should {
    "write without deadline bytes" in {
      getDeadlineIds.filter(_.isInstanceOf[BaseEntryId.GetDeadlineId]) foreach { //for all deadline ids
        deadlineID: BaseEntryId.GetDeadlineId =>
          TransientToKeyValueIdBinder.all foreach { //for all key-values
            implicit adjustedEntryId =>
              val deadlineBytes =
                DeadlineWriter.noDeadline(
                  getDeadlineId = deadlineID,
                  plusSize = 0,
                  isKeyCompressed = false
                )

              val reader = Reader(deadlineBytes)

              val expectedEntryID = adjustedEntryId.keyValueId.adjustBaseIdToKeyValueId(deadlineID.noDeadline.baseId)

              reader.readIntUnsigned().get shouldBe expectedEntryID
              DeadlineReader.NoDeadlineReader.read(reader, None).get shouldBe empty
          }
      }
    }
  }
}
