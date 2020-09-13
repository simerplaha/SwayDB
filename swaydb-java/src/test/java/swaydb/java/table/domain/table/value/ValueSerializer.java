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

package swaydb.java.table.domain.table.value;

import swaydb.data.slice.Slice;
import swaydb.data.slice.SliceReader;
import swaydb.data.util.ByteOps;
import swaydb.java.serializers.Serializer;

public class ValueSerializer implements Serializer<Value> {

  public static ValueSerializer instance = new ValueSerializer();

  @Override
  public Slice<Byte> write(Value data) {
    if (data instanceof UserValue) {
      UserValue userValue = (UserValue) data;

      return Slice
        .createJavaBytes(100)
        .add(UserValue.dataTypeId)
        .addStringUTF8WithSize(userValue.getFirstName(), ByteOps.Java())
        .addStringUTF8(userValue.getLastName(), ByteOps.Java())
        .close();
    } else if (data instanceof ProductValue) {
      ProductValue productValue = (ProductValue) data;

      return Slice
        .createJavaBytes(100)
        .add(UserValue.dataTypeId)
        .addUnsignedInt(productValue.getPrice(), ByteOps.Java())
        .close();
    } else {
      throw new IllegalStateException("Invalid TableRow type: " + data.getClass().getSimpleName());
    }
  }

  @Override
  public Value read(Slice<Byte> slice) {
    SliceReader<Byte> reader = slice.createReader(ByteOps.Java());
    byte id = reader.get();
    if (id == UserValue.dataTypeId) {
      String firstName = reader.readStringWithSizeUTF8();
      String lastName = reader.readRemainingAsStringUTF8();
      return UserValue.of(firstName, lastName);
    } else if (id == ProductValue.dataTypeId) {
      int price = reader.readUnsignedInt();
      return ProductValue.of(price);
    } else {
      throw new IllegalStateException("Invalid TableRow id: " + slice.head());
    }
  }
}
