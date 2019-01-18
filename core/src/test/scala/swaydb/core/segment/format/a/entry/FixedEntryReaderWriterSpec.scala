/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry

import org.scalatest.WordSpec
import scala.util.Random
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestTimeGenerator
import swaydb.core.TryAssert._
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class FixedEntryReaderWriterSpec extends WordSpec {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  "write and read single Fixed entry" in {
    runThis(1000.times) {
      implicit val timeGenerator = TestTimeGenerator.random
      val entry = randomFixedKeyValue(key = randomIntMax(), value = randomStringOption).toTransient
      //      println("write: " + entry)

      val read = EntryReader.read(Reader(entry.indexEntryBytes), entry.valueEntryBytes.map(Reader(_)).getOrElse(Reader.empty), 0, 0, 0, None).assertGet
      //      println("read:  " + read)
      read shouldBe entry
    }
  }

  "write and read two fixed entries" in {
    runThis(1000.times) {
      implicit val timeGenerator = TestTimeGenerator.random

      val keyValues = randomizedKeyValues(count = 1, addRandomGroups = false)
      val previous = keyValues.head

      val duplicateValues = if (Random.nextBoolean()) previous.value else randomStringOption
      val duplicateDeadline = if (Random.nextBoolean()) previous.deadline else randomDeadlineOption
      val next = randomFixedKeyValue(randomIntMax(), deadline = duplicateDeadline, value = duplicateValues).toTransient(previous = Some(previous))

      //      println("write previous: " + previous)
      //      println("write next: " + next)

      val valueBytes = Slice((previous.valueEntryBytes ++ next.valueEntryBytes).toArray)

      val previousRead = EntryReader.read(Reader(previous.indexEntryBytes), Reader(valueBytes), 0, 0, 0, None).assertGet
      previousRead shouldBe previous

      val nextRead = EntryReader.read(Reader(next.indexEntryBytes), Reader(valueBytes), 0, 0, 0, Some(previousRead)).assertGet
      nextRead shouldBe next

      //      println("read previous:  " + previousRead)
      //      println("read next:  " + nextRead)
      //      println
    }
  }
}
