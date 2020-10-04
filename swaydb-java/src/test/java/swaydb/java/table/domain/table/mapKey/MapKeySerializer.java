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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java.table.domain.table.mapKey;

import swaydb.data.slice.Slice;
import swaydb.java.serializers.Serializer;

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
    return Slice.ofBytesJava(1).add(data.getId());
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
