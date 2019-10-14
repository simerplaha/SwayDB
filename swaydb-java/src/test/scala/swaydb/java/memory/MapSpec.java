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
import scala.Option;
import scala.concurrent.duration.Deadline;
import swaydb.Apply;
import swaydb.data.util.Functions;
import swaydb.java.MapIO;
import swaydb.java.PureFunction;
import swaydb.java.data.slice.ByteSlice;
import swaydb.java.data.util.KeyVal;
import swaydb.java.serializers.Serializer;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;

class MapSpec {

  @Test
  void createMap() throws Throwable {
    MapIO<Integer, Integer, Functions.Disabled> map =
      Map
        .config(intSerializer(), intSerializer())
        .create()
        .get();

    assertDoesNotThrow(() -> map.put(1, 1).get());
    assertEquals(map.get(1).get().get(), 1);
    assertFalse(map.get(2).get().isPresent());
  }

  @Test
  void createMapWithCustomTypedComparator() throws Throwable {

    Map.Config<Integer, Integer, Functions.Disabled, Functions.Disabled> config =
      Map.config(intSerializer(), intSerializer());

    //reverse comparator
    config.setTypedComparator(Optional.of((left, right) -> left.compareTo(right) * -1));

    assertTrue(config.getTypedComparator().isPresent());

    MapIO<Integer, Integer, Functions.Disabled> map =
      config
        .create()
        .get();


    assertDoesNotThrow(() -> map.put(1, 1).get());
    assertDoesNotThrow(() -> map.put(2, 2).get());

    List<Integer> integers = map
      .stream()
      .map(KeyVal::key).materialize()
      .get();

    assertEquals(Arrays.asList(2, 1), integers);
  }

  @Test
  void createMapWithCustomSerializer() throws Throwable {
    class Key {
      Integer key;

      Key setKey(Integer key) {
        this.key = key;
        return this;
      }
    }

    class Value {
      Integer value;

      Value setValue(Integer value) {
        this.value = value;
        return this;
      }
    }

    Key key1 = new Key().setKey(1);
    Key key2 = new Key().setKey(2);

    Value value1 = new Value().setValue(1);
    Value value2 = new Value().setValue(2);

    Serializer<Key> keySerializer = new Serializer<Key>() {
      @Override
      public byte[] write(Key data) {
        byte[] bytes = {data.key.byteValue()};
        return bytes;
      }

      @Override
      public Key read(ByteSlice slice) {
        if (slice.get(0) == 1) {
          return key1;
        } else {
          return key2;
        }
      }
    };

    Serializer<Value> valueSerializer = new Serializer<Value>() {
      @Override
      public byte[] write(Value data) {
        byte[] bytes = {data.value.byteValue()};
        return bytes;
      }

      @Override
      public Value read(ByteSlice slice) {
        if (slice.get(0) == 1) {
          return value1;
        } else {
          return value2;
        }
      }
    };


    Map.Config<Key, Value, Functions.Disabled, Functions.Disabled> config =
      Map.config(keySerializer, valueSerializer);

    MapIO<Key, Value, Functions.Disabled> map =
      config
        .create()
        .get();


    assertDoesNotThrow(() -> map.put(key1, value1).get());
    assertDoesNotThrow(() -> map.put(key2, value2).get());

    List<Key> mapKeys =
      map
        .stream()
        .map(KeyVal::key).materialize()
        .get();

    assertEquals(Arrays.asList(key1, key2), mapKeys);

    List<Integer> setKeys =
      map
        .keys()
        .stream()
        .map(key -> key.key)
        .materialize()
        .get();

    assertEquals(Arrays.asList(1, 2), setKeys);
  }


  @Test
  void registerAndApplyFunction() throws Throwable {
    MapIO<Integer, Integer, PureFunction<Integer, Integer>> map =
      Map
        .configWithFunctions(intSerializer(), intSerializer())
        .create()
        .get();

    assertDoesNotThrow(() -> map.put(1, 1).get());
    assertEquals(map.get(1).get().get(), 1);


    PureFunction.GetKey<Integer, Integer> getKey =
      (key, deadline) ->
        swaydb.java.Apply.update(10, Optional.empty());


    map.registerFunction(getKey).get();
    map.applyFunction(1, getKey).get();

    Integer integer = map.get(1).get().get();
    System.out.println(integer);
  }
}
