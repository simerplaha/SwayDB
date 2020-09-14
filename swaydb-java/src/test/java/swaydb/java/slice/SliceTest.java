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

import java.util.ArrayList;
import java.util.Arrays;

import static swaydb.data.java.CommonAssertions.*;

public class SliceTest {

  @Test
  void serialising() {
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

    //does not work without casting.
    final Byte[] bytes1 = (Byte[]) bytes.toArrayCopy();
    shouldBeSameIterators(Arrays.stream(bytes1).iterator(), bytes.asJava().iterator());
  }

  @Test
  void createFromArray() {
    Byte[] array = {1, 2, 3, 4, 5, 127};
    Slice<Byte> bytes = Slice.createForJava(array);

    final ArrayList<Byte> actual = new ArrayList<>();
    bytes.asJava().forEach(actual::add);

    shouldBeSameIterators(actual.iterator(), bytes.asJava().iterator());
  }

  @Test
  void writeAndRead() {
    //boolean
    shouldBeTrue(Slice.writeBoolean(true, ByteOps.Java()).readBoolean());
    shouldBeFalse(Slice.writeBoolean(false, ByteOps.Java()).readBoolean());
    //Integer
    shouldBe(Slice.writeInt(Int.MaxValue(), ByteOps.Java()).readInt(ByteOps.Java()), Int.MaxValue());
    shouldBe(Slice.writeInt(10000, ByteOps.Java()).readInt(ByteOps.Java()), 10000);
    shouldBe(Slice.writeInt(Int.MinValue(), ByteOps.Java()).readInt(ByteOps.Java()), Int.MinValue());
    shouldBe(Slice.writeUnsignedInt(Int.MaxValue(), ByteOps.Java()).readUnsignedInt(ByteOps.Java()), Int.MaxValue());

    shouldBe(Slice.writeLong(Long.MIN_VALUE, ByteOps.Java()).readLong(ByteOps.Java()), Long.MIN_VALUE);
    shouldBe(Slice.writeLong(Long.MAX_VALUE, ByteOps.Java()).readLong(ByteOps.Java()), Long.MAX_VALUE);
    shouldBe(Slice.writeUnsignedLong(Long.MAX_VALUE, ByteOps.Java()).readUnsignedLong(ByteOps.Java()), Long.MAX_VALUE);

    shouldBe(Slice.writeStringUTF8("test string", ByteOps.Java()).readStringUTF8(ByteOps.Java()), "test string");
  }
}
