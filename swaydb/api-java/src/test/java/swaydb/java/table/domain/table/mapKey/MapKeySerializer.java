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

package swaydb.java.table.domain.table.mapKey;

import swaydb.java.serializers.Serializer;
import swaydb.slice.Slice;

/**
 * Serializes MapKey.
 */
public class MapKeySerializer implements Serializer<MapKey> {

  public static MapKeySerializer instance = new MapKeySerializer();

  //we don't need multiple instances of Table types.
  UsersMap userTable = new UsersMap();
  ProductsMap productTable = new ProductsMap();

  @Override
  public Slice<Byte> write(MapKey data) {
    //we only need to store the Id. So create a byte array
    //of size 1 and add the id.
    return Slice.jOfBytes(1).add(data.getId());
  }

  @Override
  public MapKey read(Slice<Byte> slice) {
    //Get the head id. If it's User's id then return user table else product
    if (slice.head().equals(userTable.getId())) {
      return userTable;
    } else if (slice.head().equals(productTable.getId())) {
      return productTable;
    } else {
      throw new IllegalStateException("Invalid Table id: " + slice.head());
    }
  }
}
