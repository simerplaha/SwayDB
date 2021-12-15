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

package swaydb.java.table.domain.table.key;

import swaydb.java.serializers.Serializer;
import swaydb.slice.Slice;
import swaydb.slice.SliceReader;
import swaydb.slice.utils.ByteOps;

public class KeySerializer implements Serializer<Key> {

  public static KeySerializer instance = new KeySerializer();

  @Override
  public Slice<Byte> write(Key data) {
    if (data instanceof UserKey) {
      UserKey userRow = (UserKey) data;

      return Slice
        .jOfBytes(100)
        .add(UserKey.dataTypeId)
        .addStringUTF8(userRow.getEmail(), ByteOps.Java())
        .close();
    } else if (data instanceof ProductKey) {
      ProductKey productRow = (ProductKey) data;

      return Slice
        .jOfBytes(100)
        .add(ProductKey.dataTypeId)
        .addUnsignedInt(productRow.getId(), ByteOps.Java())
        .close();
    } else {
      throw new IllegalStateException("Invalid PrimaryKey type: " + data.getClass().getSimpleName());
    }
  }

  @Override
  public Key read(Slice<Byte> slice) {
    SliceReader<Byte> reader = slice.createReader(ByteOps.Java());

    byte id = reader.get();

    if (id == UserKey.dataTypeId) {
      //
      return UserKey.of(reader.readRemainingAsStringUTF8());
    } else if (id == ProductKey.dataTypeId) {
      //
      return ProductKey.of(reader.readUnsignedInt());
    } else {
      //
      throw new IllegalStateException("Invalid PrimaryKey id: " + slice.head());
    }
  }
}
