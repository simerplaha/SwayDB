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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java;

import org.junit.jupiter.api.Test;
import swaydb.*;
import swaydb.data.java.JavaEventually;
import swaydb.data.java.TestBase;
import swaydb.java.memory.MemoryMap;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;

abstract class MapFunctionsOnTest extends TestBase implements JavaEventually {

  public abstract <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                                Serializer<V> valueSerializer,
                                                                                List<PureFunction<K, V, Apply.Map<V>>> functions) throws IOException;

  public abstract <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                                Serializer<V> valueSerializer,
                                                                                List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                                KeyComparator<K> keyComparator) throws IOException;


  @Test
  void registerAndApplyFunction() throws IOException {
    PureFunction.OnKey<Integer, Integer, Apply.Map<Integer>> updateValueTo10 =
      (key, deadline) ->
        Apply.update(10);

    PureFunction.OnKeyValue<Integer, Integer, Apply.Map<Integer>> incrementBy1 =
      (key, value, deadline) ->
        Apply.update(value + 1);

    PureFunction.OnKeyValue<Integer, Integer, Apply.Map<Integer>> removeMod0OrIncrementBy1 =
      (key, value, deadline) -> {
        if (key % 10 == 0) {
          return Apply.removeFromMap();
        } else {
          return Apply.update(value + 1);
        }
      };

    //this will not compile since the return type specified is a Set - expected!
    PureFunction.OnKeyValue<Integer, String, Apply.Map<String>> invalidType =
      (key, value, deadline) ->
        Apply.update(value + 1);

    MapT<Integer, Integer, PureFunction<Integer, Integer, Apply.Map<Integer>>> map =
      createMap(intSerializer(), intSerializer(), Arrays.asList(updateValueTo10, incrementBy1, removeMod0OrIncrementBy1));

    map.put(Stream.range(1, 100).map(KeyVal::create));

    map.applyFunction(1, updateValueTo10);
    assertEquals(10, map.get(1).get());

    map.applyFunction(10, 20, incrementBy1);
    IntStream
      .rangeClosed(10, 20)
      .forEach(
        integer ->
          assertEquals(integer + 1, map.get(integer).get())
      );

    map.applyFunction(21, 50, removeMod0OrIncrementBy1);
    IntStream
      .rangeClosed(21, 50)
      .forEach(
        integer -> {
          if (integer % 10 == 0) {
            assertFalse(map.get(integer).isPresent());
          } else {
            assertEquals(integer + 1, map.get(integer).get());
          }
        }
      );

    //untouched 51 - 100. Overlapping functions executions.
    map.commit(
      Arrays.asList(
        Prepare.applyMapFunction(51, updateValueTo10),
        Prepare.applyMapFunction(52, 100, updateValueTo10),
        Prepare.applyMapFunction(51, 100, incrementBy1),
        Prepare.applyMapFunction(51, 100, removeMod0OrIncrementBy1)
      )
    );

    assertEquals(12, map.get(51).get());
    assertFalse(map.get(60).isPresent());
    assertFalse(map.get(70).isPresent());
    assertFalse(map.get(80).isPresent());
    assertFalse(map.get(90).isPresent());
    assertFalse(map.get(100).isPresent());

    map.delete();
  }
}
