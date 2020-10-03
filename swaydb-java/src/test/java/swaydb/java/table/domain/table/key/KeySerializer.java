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

package swaydb.java.table.domain.table.key;

import swaydb.data.slice.Slice;
import swaydb.data.slice.SliceReader;
import swaydb.data.util.ByteOps;
import swaydb.java.serializers.Serializer;

public class KeySerializer implements Serializer<Key> {

  public static KeySerializer instance = new KeySerializer();

  @Override
  public Slice<Byte> write(Key data) {
    if (data instanceof UserKey) {
      UserKey userRow = (UserKey) data;

      return Slice
        .ofBytesJava(100)
        .add(UserKey.dataTypeId)
        .addStringUTF8(userRow.getEmail(), ByteOps.Java())
        .close();
    } else if (data instanceof ProductKey) {
      ProductKey productRow = (ProductKey) data;

      return Slice
        .ofBytesJava(100)
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
