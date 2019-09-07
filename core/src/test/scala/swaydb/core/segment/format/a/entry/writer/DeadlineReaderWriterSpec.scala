///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// *  it under the terms of the GNU Affero General Public License as
// *  published by the Free Software Foundation, either version 3 of the
// *  License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a.entry.writer
//
//import java.util.concurrent.TimeUnit
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.Error.Segment.ExceptionHandler
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.io.reader.Reader
//import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
//import swaydb.core.segment.format.a.entry.reader.DeadlineReader
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.concurrent.duration._
//
//class DeadlineReaderWriterSpec extends WordSpec with Matchers {
//
//  val getDeadlineIds =
//    allBaseEntryIds collect {
//      case entryId: BaseEntryId.DeadlineId =>
//        entryId
//    }
//
//  getDeadlineIds should not be empty
//
//  def runTestForEachDeadlineAndBinder(testFunction: PartialFunction[(BaseEntryId.DeadlineId, TransientToKeyValueIdBinder[_]), Unit]): Unit =
//    getDeadlineIds.filter(_.isInstanceOf[BaseEntryId.DeadlineId]) foreach { //for all deadline ids
//      deadlineID: BaseEntryId.DeadlineId =>
//        TransientToKeyValueIdBinder.allBinders foreach { //for all key-values
//          binder: TransientToKeyValueIdBinder[_] =>
//            testFunction(deadlineID, binder)
//        }
//    }
//
//  "applyDeadlineId" should {
//    "compress compress deadlines" in {
//      getDeadlineIds foreach {
//        entryId =>
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.OneCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(1, entryId) shouldBe true
//
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.TwoCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(2, entryId) shouldBe true
//
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.ThreeCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(3, entryId) shouldBe true
//
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.FourCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(4, entryId) shouldBe true
//
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.FiveCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(5, entryId) shouldBe true
//
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.SixCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(6, entryId) shouldBe true
//
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.SevenCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(7, entryId) shouldBe true
//
//          allBaseEntryIds collect {
//            case entryId: BaseEntryId.Deadline.FullyCompressed =>
//              entryId
//          } contains DeadlineWriter.applyDeadlineId(8, entryId) shouldBe true
//
//          assertThrows[Exception] {
//            DeadlineWriter.applyDeadlineId(randomIntMax(100) + 9, entryId) shouldBe true
//          }
//      }
//    }
//  }
//
//  "uncompress" should {
//    "write deadline as uncompressed" in {
//      runTestForEachDeadlineAndBinder {
//        case (deadlineId, keyValueBinder) =>
//          implicit val binder = keyValueBinder
//
//          def doAssert(isKeyCompressed: Boolean, hasCompression: Boolean) = {
//            val deadline = 10.seconds.fromNow
//            val (deadlineBytes, isPrefixCompressed) =
//              DeadlineWriter.uncompressed(
//                currentDeadline = deadline,
//                deadlineId = deadlineId,
//                plusSize = 0,
//                hasPrefixCompressed = hasCompression,
//                isKeyCompressed = isKeyCompressed
//              )
//
//            deadlineBytes.isFull shouldBe true
//            val reader = Reader[swaydb.Error.Segment](deadlineBytes)
//
//            isPrefixCompressed shouldBe (hasCompression || isKeyCompressed)
//
//            val expectedKeyValueId = binder.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineId.deadlineUncompressed.baseId, isKeyCompressed)
//
//            reader.readIntUnsigned().get shouldBe expectedKeyValueId
//            DeadlineReader.DeadlineUncompressedReader.read(reader, None).get should contain(deadline)
//          }
//
//          doAssert(isKeyCompressed = true, hasCompression = true)
//          doAssert(isKeyCompressed = true, hasCompression = false)
//          doAssert(isKeyCompressed = false, hasCompression = true)
//          doAssert(isKeyCompressed = false, hasCompression = false)
//      }
//    }
//  }
//
//  "compress" should {
//    "write compressed deadlines with previous deadline" in {
//      implicit def bytesToDeadline(bytes: Slice[Byte]): FiniteDuration = FiniteDuration(bytes.readLong(), TimeUnit.NANOSECONDS)
//
//      runTestForEachDeadlineAndBinder {
//        case (deadlineId, keyValueBinder) =>
//          implicit val binder = keyValueBinder
//          //Test for when there are zero compressed bytes, compression should return None.
//          DeadlineWriter.compress(
//            currentDeadline = Deadline(Slice.fill[Byte](8)(0.toByte)),
//            previousDeadline = Deadline(Slice.fill[Byte](8)(1.toByte)),
//            deadlineId = deadlineId,
//            plusSize = 0,
//            isKeyCompressed = false
//          ) shouldBe empty
//
//          //Test for when there are compressed bytes.
//          (1 to 8) foreach { //for some or all deadline bytes compressed.
//            commonBytes =>
//              val currentDeadlineBytes =
//                Slice.fill[Byte](commonBytes)(0.toByte) ++ Slice.fill[Byte](8 - commonBytes)(1.toByte)
//
//              val previousDeadlineBytes =
//                Slice.fill[Byte](commonBytes)(0.toByte) ++ Slice.fill[Byte](8 - commonBytes)(2.toByte)
//
//              val currentDeadline = Deadline(currentDeadlineBytes)
//              val previousDeadline = Deadline(previousDeadlineBytes)
//
//              //test with both when the key is compressed and uncompressed.
//              def doAssert(compressedKey: Boolean) = {
//                val deadlineBytesOption =
//                  DeadlineWriter.compress(
//                    currentDeadline = currentDeadline,
//                    previousDeadline = previousDeadline,
//                    deadlineId = deadlineId,
//                    plusSize = 0,
//                    isKeyCompressed = compressedKey
//                  )
//
//                deadlineBytesOption shouldBe defined
//
//                val (deadlineBytes, isPrefixCompressed) = deadlineBytesOption.get
//                deadlineBytes.isFull shouldBe true
//                isPrefixCompressed shouldBe true
//
//                val reader = Reader[swaydb.Error.Segment](deadlineBytes)
//
//                def keyValueIdAdjuster(baseId: Int) =
//                  if (compressedKey)
//                    binder.keyValueId.adjustBaseIdToKeyValueIdKey_Compressed(baseId)
//                  else
//                    binder.keyValueId.adjustBaseIdToKeyValueIdKey_UnCompressed(baseId)
//
//                val (expectedKeyValueId: Int, deadlineReader: DeadlineReader[_]) =
//                  if (commonBytes == 1)
//                    (keyValueIdAdjuster(deadlineId.deadlineOneCompressed.baseId), DeadlineReader.DeadlineOneCompressedReader)
//                  else if (commonBytes == 2)
//                    (keyValueIdAdjuster(deadlineId.deadlineTwoCompressed.baseId), DeadlineReader.DeadlineTwoCompressedReader)
//                  else if (commonBytes == 3)
//                    (keyValueIdAdjuster(deadlineId.deadlineThreeCompressed.baseId), DeadlineReader.DeadlineThreeCompressedReader)
//                  else if (commonBytes == 4)
//                    (keyValueIdAdjuster(deadlineId.deadlineFourCompressed.baseId), DeadlineReader.DeadlineFourCompressedReader)
//                  else if (commonBytes == 5)
//                    (keyValueIdAdjuster(deadlineId.deadlineFiveCompressed.baseId), DeadlineReader.DeadlineFiveCompressedReader)
//                  else if (commonBytes == 6)
//                    (keyValueIdAdjuster(deadlineId.deadlineSixCompressed.baseId), DeadlineReader.DeadlineSixCompressedReader)
//                  else if (commonBytes == 7)
//                    (keyValueIdAdjuster(deadlineId.deadlineSevenCompressed.baseId), DeadlineReader.DeadlineSevenCompressedReader)
//                  else if (commonBytes == 8)
//                    (keyValueIdAdjuster(deadlineId.deadlineFullyCompressed.baseId), DeadlineReader.DeadlineFullyCompressedReader)
//                  else
//                    fail("Cannot have more than 8 common bytes")
//
//                reader.readIntUnsigned().get shouldBe expectedKeyValueId
//
//                deadlineReader
//                  .read(
//                    indexReader = reader,
//                    previous = Some(randomPutKeyValue(key = 1, deadline = Some(previousDeadline)))
//                  ).get should contain(currentDeadline)
//              }
//
//              doAssert(compressedKey = true)
//              doAssert(compressedKey = false)
//          }
//      }
//    }
//  }
//
//  "noDeadline" should {
//    "write without deadline bytes" in {
//      getDeadlineIds.filter(_.isInstanceOf[BaseEntryId.DeadlineId]) foreach { //for all deadline ids
//        deadlineID: BaseEntryId.DeadlineId =>
//          TransientToKeyValueIdBinder.allBinders foreach { //for all key-values
//            implicit binder =>
//              def doAssert(isKeyCompressed: Boolean, hasPrefixCompressed: Boolean) = {
//
//                val (deadlineBytes, isPrefixCompressed) =
//                  DeadlineWriter.noDeadline(
//                    deadlineId = deadlineID,
//                    plusSize = 0,
//                    hasPrefixCompressed = hasPrefixCompressed,
//                    isKeyCompressed = isKeyCompressed
//                  )
//
//                isPrefixCompressed shouldBe (hasPrefixCompressed || isKeyCompressed)
//
//                val expectedEntryID = binder.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineID.noDeadline.baseId, isKeyCompressed)
//
//                val reader = Reader[swaydb.Error.Segment](deadlineBytes)
//                deadlineBytes.isFull shouldBe true
//                reader.readIntUnsigned().get shouldBe expectedEntryID
//                DeadlineReader.NoDeadlineReader.read(reader, None).get shouldBe empty
//              }
//
//              doAssert(isKeyCompressed = true, hasPrefixCompressed = true)
//              doAssert(isKeyCompressed = true, hasPrefixCompressed = false)
//              doAssert(isKeyCompressed = false, hasPrefixCompressed = true)
//              doAssert(isKeyCompressed = false, hasPrefixCompressed = false)
//          }
//      }
//    }
//  }
//
//  "write" should {
//    "write uncompressed if enablePrefixCompression is false" in {
//      runTestForEachDeadlineAndBinder {
//        case (deadlineID, keyValueBinder) =>
//          implicit val binder = keyValueBinder
//
//          def doAssert(isKeyCompressed: Boolean, hasPrefixCompressed: Boolean) = {
//
//            val deadline = Some(randomDeadline())
//            val (deadlineBytes, isPrefixCompressed) =
//              DeadlineWriter.write(
//                currentDeadline = deadline,
//                previousDeadline = deadline,
//                deadlineId = deadlineID,
//                enablePrefixCompression = false,
//                plusSize = 0,
//                isKeyCompressed = isKeyCompressed,
//                hasPrefixCompressed = hasPrefixCompressed
//              )
//
//            isPrefixCompressed shouldBe (isKeyCompressed || hasPrefixCompressed)
//
//            val expectedEntryID = binder.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineID.deadlineUncompressed.baseId, isKeyCompressed)
//
//            val reader = Reader[swaydb.Error.Segment](deadlineBytes)
//            deadlineBytes.isFull shouldBe true
//            reader.readIntUnsigned().get shouldBe expectedEntryID
//            DeadlineReader.DeadlineUncompressedReader.read(reader, None).get shouldBe deadline
//          }
//
//          doAssert(isKeyCompressed = true, hasPrefixCompressed = true)
//          doAssert(isKeyCompressed = true, hasPrefixCompressed = false)
//          doAssert(isKeyCompressed = false, hasPrefixCompressed = true)
//          doAssert(isKeyCompressed = false, hasPrefixCompressed = false)
//      }
//    }
//
//    "write no deadline if enablePrefixCompression is true but has no deadline" in {
//      runTestForEachDeadlineAndBinder {
//        case (deadlineID, keyValueBinder) =>
//          implicit val binder = keyValueBinder
//
//          def doAssert(isKeyCompressed: Boolean, hasPrefixCompressed: Boolean) = {
//
//            val (deadlineBytes, isPrefixCompressed) =
//              DeadlineWriter.write(
//                currentDeadline = None,
//                previousDeadline = randomDeadlineOption(),
//                deadlineId = deadlineID,
//                enablePrefixCompression = randomBoolean(),
//                plusSize = 0,
//                isKeyCompressed = isKeyCompressed,
//                hasPrefixCompressed = hasPrefixCompressed
//              )
//
//            isPrefixCompressed shouldBe (isKeyCompressed || hasPrefixCompressed)
//
//            val expectedEntryID = binder.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineID.noDeadline.baseId, isKeyCompressed)
//
//            val reader = Reader[swaydb.Error.Segment](deadlineBytes)
//            deadlineBytes.isFull shouldBe true
//            reader.readIntUnsigned().get shouldBe expectedEntryID
//            DeadlineReader.NoDeadlineReader.read(reader, None).get shouldBe empty
//          }
//
//          runThis(10.times) {
//            doAssert(isKeyCompressed = true, hasPrefixCompressed = true)
//            doAssert(isKeyCompressed = true, hasPrefixCompressed = false)
//            doAssert(isKeyCompressed = false, hasPrefixCompressed = true)
//            doAssert(isKeyCompressed = false, hasPrefixCompressed = false)
//          }
//      }
//    }
//
//    "write compress deadline" in {
//      runTestForEachDeadlineAndBinder {
//        case (deadlineID, keyValueBinder) =>
//          implicit val binder = keyValueBinder
//
//          def doAssert(isKeyCompressed: Boolean, hasPrefixCompressed: Boolean) = {
//
//            val deadline = Some(randomDeadline())
//
//            val (deadlineBytes, isPrefixCompressed) =
//              DeadlineWriter.write(
//                currentDeadline = deadline,
//                previousDeadline = deadline,
//                deadlineId = deadlineID,
//                enablePrefixCompression = true,
//                plusSize = 0,
//                isKeyCompressed = isKeyCompressed,
//                hasPrefixCompressed = hasPrefixCompressed
//              )
//
//            isPrefixCompressed shouldBe true
//
//            val expectedEntryID = binder.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineID.deadlineFullyCompressed.baseId, isKeyCompressed)
//
//            val reader = Reader[swaydb.Error.Segment](deadlineBytes)
//            deadlineBytes.isFull shouldBe true
//            reader.readIntUnsigned().get shouldBe expectedEntryID
//            val previous = randomFixedKeyValue(key = 1, deadline = deadline, includeFunctions = false)
//            DeadlineReader.DeadlineFullyCompressedReader.read(reader, Some(previous)).get shouldBe deadline
//          }
//
//          runThis(10.times) {
//            doAssert(isKeyCompressed = true, hasPrefixCompressed = true)
//            doAssert(isKeyCompressed = true, hasPrefixCompressed = false)
//            doAssert(isKeyCompressed = false, hasPrefixCompressed = true)
//            doAssert(isKeyCompressed = false, hasPrefixCompressed = false)
//          }
//      }
//    }
//  }
//}
