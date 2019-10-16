/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.java.memory;


import org.junit.jupiter.api.Test;
import swaydb.java.PureFunction;
import swaydb.java.Return;
import swaydb.java.SetIO;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;

class SetSpec {

  @Test
  void createMap() {
    SetIO<Integer, Void> set =
      Set
        .config(intSerializer())
        .init()
        .get();

    assertDoesNotThrow(() -> set.add(1).get());
    assertEquals(set.get(1).get().get(), 1);
    assertFalse(set.get(2).get().isPresent());

    set
      .forEach(integer -> System.out.println("integer = " + integer))
      .materialize()
      .get();


    PureFunction.OnKey<Integer, Void, Return.Set<Void>> getKey = (key, deadline) -> null;
//    set.registerFunction(getKey); //does not compile
  }

  @Test
  void createMapWithFunctions() {
    SetIO<Integer, PureFunction<Integer, Void, Return.Set<Void>>> set =
      Set
        .configWithFunctions(intSerializer())
        .init()
        .get();

    set.close().get();

    assertDoesNotThrow(() -> set.add(1).get());
    assertEquals(set.get(1).get().get(), 1);
    assertFalse(set.get(2).get().isPresent());

    set
      .forEach(integer -> System.out.println("integer = " + integer))
      .materialize()
      .get();

    PureFunction.OnKey<Integer, Void, Return.Set<Void>> removeKey =
      (key, deadline) ->
        Return.remove();

    set.registerFunction(removeKey).get();

    set.applyFunction(1, removeKey).get();
    assertTrue(set.isEmpty().get());
  }


}
