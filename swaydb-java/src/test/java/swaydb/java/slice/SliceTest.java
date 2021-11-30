/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.java.slice;

import org.junit.jupiter.api.Test;
import scala.Int;
import scala.reflect.ClassTag;
import swaydb.slice.Slice;
import swaydb.slice.SliceMut;
import swaydb.slice.SliceReader;
import swaydb.slice.utils.ByteOps;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static swaydb.java.JavaTest.*;

public class SliceTest {
  @Test
  void empty() {
    shouldBeEmpty(Slice.jEmptyBytes()); //Bytes
    shouldBeEmpty(Slice.jEmpty(Integer.class)); //dynamic class

    class MyClass {
    }
    shouldBeEmpty(Slice.jEmpty(MyClass.class)); //dynamic class
  }

  @Test
  void forEach() {
    Slice.jFillBytes(10, () -> Byte.MAX_VALUE).jForEach(System.out::println);
  }

  @Test
  void filterWhenAllRemoved() {
    Slice<Byte> byteSlice =
      Slice
        .jFillBytes(10, () -> Byte.MAX_VALUE)
        .jFilter(b -> false);

    shouldBeEmpty(byteSlice);
  }

  @Test
  void filterWhenAllKept() {
    Slice<Byte> byteSlice =
      Slice
        .jFillBytes(10, () -> Byte.MAX_VALUE)
        .jFilter(b -> true);

    shouldBe(byteSlice, Slice.jFillBytes(10, () -> Byte.MAX_VALUE));
  }

  @Test
  void addIntToByteSlice() {
    SliceMut<Byte> slice = Slice.jOfBytes(10).jAddInt(1);
    shouldBe(slice, Slice.jWriteInt(1));
  }

  @Test
  void serialising() {
    final Slice<Byte> bytes =
      Slice
        .jOfBytes(100)
        .add(Byte.MIN_VALUE)
        .add(Byte.MAX_VALUE)
        .addAll(Slice.jOfBytes(0))
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
    Slice<Byte> bytes = Slice.jOf(array);

    final ArrayList<Byte> actual = new ArrayList<>();
    bytes.asJava().forEach(actual::add);

    shouldBeSameIterators(actual.iterator(), bytes.asJava().iterator());
  }

  @Test
  void writeAndRead() {
    //boolean
    shouldBeTrue(Slice.writeBoolean(true, ByteOps.Java()).readBoolean(ByteOps.Java()));
    shouldBeFalse(Slice.writeBoolean(false, ByteOps.Java()).readBoolean(ByteOps.Java()));
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

  /**
   * Tests <a href="https://github.com/simerplaha/SwayDB/issues/308">Issue 308</a>
   * to create Slice from native java byte array.
   */
  @Test
  void createSliceFromPrimitiveByteArray() {
    byte[] array = {1, 2, 3, 4, 5, 127};
    Slice<Byte> slice = Slice.jOf(array);
    shouldBe(slice.head(), array[0]);
    shouldBe(slice.get(1), array[1]);
    shouldBe(slice.get(2), array[2]);
    shouldBe(slice.get(3), array[3]);
    shouldBe(slice.get(4), array[4]);
    shouldBe(slice.get(5), array[5]);
  }

  @Test
  void toByteArray() {
    byte[] writeBytes = {1, 2, 3, 4, 5, 127};
    Slice<Byte> slice = Slice.jOf(writeBytes);

    byte[] readBytes = slice.toByteArray();
    shouldBe(writeBytes, readBytes);
  }

  @Test
  void toByteArrayThrowsClassCastException() {
    //There is no need in Java to create Slice of types other than Slice<Byte>
    //so generic Slice.of<T> does not exist for Java yet hence the explicit instantiation
    //of a Slice<Integer> type using Scala's ClassTag.
    Slice<Object> add = Slice.of(3, false, ClassTag.Int()).add(1).add(2).add(3);
    //cannot convert Slice<Integer> array to Slice<Byte> so expect ClassCastException.
    assertThrows(ClassCastException.class, add::toByteArray);
  }
}
