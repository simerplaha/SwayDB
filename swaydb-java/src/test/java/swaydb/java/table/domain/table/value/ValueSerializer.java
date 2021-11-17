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

package swaydb.java.table.domain.table.value;

import swaydb.java.serializers.Serializer;
import swaydb.slice.Slice;
import swaydb.slice.SliceReader;
import swaydb.slice.utils.ByteOps;

public class ValueSerializer implements Serializer<Value> {

  public static ValueSerializer instance = new ValueSerializer();

  @Override
  public Slice<Byte> write(Value data) {
    if (data instanceof UserValue) {
      UserValue userValue = (UserValue) data;

      return Slice
        .ofBytesJava(100)
        .add(UserValue.dataTypeId)
        .addStringUTF8WithSize(userValue.getFirstName(), ByteOps.Java())
        .addStringUTF8(userValue.getLastName(), ByteOps.Java())
        .close();
    } else if (data instanceof ProductValue) {
      ProductValue productValue = (ProductValue) data;

      return Slice
        .ofBytesJava(100)
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
