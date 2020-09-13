/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.java.slice;

import org.junit.jupiter.api.Test;
import scala.Int;
import swaydb.data.slice.Slice;
import swaydb.data.slice.SliceReader;
import swaydb.data.util.ByteOps;

import static swaydb.data.java.CommonAssertions.*;

public class SliceTest {

  @Test
  void sliceTest() {
    final Slice<Byte> bytes =
      Slice
        .createJavaBytes(100)
        .add(Byte.MIN_VALUE)
        .add(Byte.MAX_VALUE)
        .addAll(Slice.createJavaBytes(0))
        .addUnsignedInt(Int.MaxValue(), ByteOps.Java())
        .addUnsignedLong(Long.MIN_VALUE, ByteOps.Java())
        .addSignedInt(Int.MinValue(), ByteOps.Java())
        .addSignedInt(Int.MaxValue(), ByteOps.Java())
        .addStringUTF8("this is a test string", ByteOps.Java());

    SliceReader<Byte> reader = bytes.createReader(ByteOps.Java());

    shouldBe(reader.get(), Byte.MIN_VALUE);
    shouldBe(reader.get(), Byte.MAX_VALUE);
    shouldBe(reader.readUnsignedInt(), Int.MaxValue());
    shouldBe(reader.readUnsignedLong(), Long.MIN_VALUE);
    shouldBe(reader.readSignedInt(), Int.MinValue());
    shouldBe(reader.readSignedInt(), Int.MaxValue());
    shouldBe(reader.readRemainingAsStringUTF8(), "this is a test string");
    shouldBeFalse(reader.hasMore());

    bytes.asJava().forEach(System.out::println);
  }
}
