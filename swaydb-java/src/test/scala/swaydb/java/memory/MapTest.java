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


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import swaydb.Prepare;
import swaydb.data.util.Functions;
import swaydb.java.*;
import swaydb.java.data.slice.ByteSlice;
import swaydb.java.data.util.KeyVal;
import swaydb.java.data.util.Pair;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;


class MemoryMapFunctionsDisabledTest extends MapTest {

  public <K, V> MapIO<K, V, Functions.Disabled> createMap(Serializer<K> keySerializer,
                                                          Serializer<V> valueSerializer) {
    MapIO<K, V, Functions.Disabled> map =
      swaydb.java.memory.Map
        .config(keySerializer, valueSerializer)
        .init()
        .get();

    return map;
  }
}

class PersistentMapFunctionsDisabledTest extends MapTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> MapIO<K, V, Functions.Disabled> createMap(Serializer<K> keySerializer,
                                                          Serializer<V> valueSerializer) throws IOException {
    MapIO<K, V, Functions.Disabled> map =
      swaydb.java.persistent.Map
        .config(testDir(), keySerializer, valueSerializer)
        .init()
        .get();

    return map;
  }
}

abstract class MapTest extends TestBase implements JavaEventually {

  public abstract <K, V> MapIO<K, V, Functions.Disabled> createMap(Serializer<K> keySerializer,
                                                                   Serializer<V> valueSerializer) throws IOException;

  @Test
  void putTest() throws IOException {
    MapIO<Integer, Integer, ?> map = createMap(intSerializer(), intSerializer());

    map.put(1, 1).get();
    map.put(2, 2).get();
    map.put(3, 3, Duration.ofSeconds(2)).get();
    //list
    map.put(Arrays.asList(new KeyVal<>(4, 4), new KeyVal<>(5, 5))).get();
    //same with iterator
    map.put(Arrays.asList(new KeyVal<>(6, 6), new KeyVal<>(7, 7)).iterator()).get();
    map.put(Stream.create(Arrays.asList(new KeyVal<>(8, 8), new KeyVal<>(9, 9)))).get();

    HashMap<Integer, Integer> actualKeyValues = new HashMap<>();

    map
      .forEach(
        keyValue ->
          actualKeyValues.put(keyValue.key(), keyValue.value())
      )
      .materialize()
      .get();

    HashMap<Integer, Integer> expectedKeyValues = new HashMap<>();

    IntStream
      .rangeClosed(1, 9)
      .forEach(
        integer ->
          expectedKeyValues.put(integer, integer)
      );

    //contains test
    IntStream
      .rangeClosed(1, 9)
      .forEach(
        integer -> {
          assertTrue(map.contains(integer).get());
          assertTrue(map.mightContain(integer).get());
        }
      );

    assertEquals(9, actualKeyValues.size());
    assertEquals(expectedKeyValues, actualKeyValues);

    eventuallyInSeconds(3, (Supplier<Void>) () -> {
      assertFalse(map.get(3).get().isPresent());
      return null;
    });
  }

  @Test
  void expireTest() throws IOException {
    MapIO<Integer, Integer, ?> map = createMap(intSerializer(), intSerializer());

    Duration expireAfter = Duration.ofSeconds(2);

    //put and then expire
    map.put(1, 1).get();
    map.expire(1, expireAfter).get();

    //put expire
    map.put(2, 2, expireAfter).get();

    //put list and expire list
    map.put(Arrays.asList(new KeyVal<>(3, 3), new KeyVal<>(4, 4))).get();
    map.expire(Arrays.asList(new Pair<>(3, expireAfter), new Pair<>(4, expireAfter)).iterator()).get();

    //put list and expire stream
    map.put(Arrays.asList(new KeyVal<>(5, 5), new KeyVal<>(6, 6))).get();
    map.expire(Stream.create(Arrays.asList(new Pair<>(5, expireAfter), new Pair<>(6, expireAfter)))).get();

    assertEquals(6, map.size().get());

    eventuallyInSeconds(3, (Supplier<Void>) () -> {
      assertTrue(map.isEmpty().get());
      assertEquals(0, map.size().get());

      IntStream
        .rangeClosed(1, 6)
        .forEach(
          integer ->
            assertFalse(map.get(integer).get().isPresent())
        );

      return null;
    });
  }

  @Test
  void expireRangeShouldClearAllKeyValuesTest() throws IOException {
    MapIO<Integer, Integer, ?> map = createMap(intSerializer(), intSerializer());

    int maxKeyValues = 10000;

    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(
        integer ->
          map.put(integer, integer).get()
      );

    //contains test
    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(
        integer -> {
          assertTrue(map.contains(integer).get());
          assertTrue(map.mightContain(integer).get());
        }
      );

    assertEquals(maxKeyValues, map.size().get());

    //expire individually
    IntStream
      .rangeClosed(1, maxKeyValues / 2)
      .forEach(
        value ->
          map.expire(value, Duration.ofSeconds(1))
      );

    //expire range.
    map.expire(maxKeyValues / 2, maxKeyValues, Duration.ofSeconds(1)).get();

    eventuallyInSeconds(
      2,
      (Supplier<Void>) () -> {
        assertEquals(0, map.size().get());
        assertTrue(map.isEmpty().get());
        return null;
      }
    );
  }


  @Test
  void updateTest() throws IOException {
    MapIO<Integer, Integer, ?> map = createMap(intSerializer(), intSerializer());

    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          map.put(integer, integer).get()
      );

    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          map.update(integer, integer + 1).get()
      );

    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          assertEquals(integer + 1, map.get(integer).get().get())
      );
  }

  @Test
  void clearTest() throws IOException {
    MapIO<Integer, Integer, ?> map = createMap(intSerializer(), intSerializer());

    IntStream
      .rangeClosed(1, 100000)
      .forEach(
        integer ->
          map.put(integer, integer).get()
      );

    assertEquals(100000, map.size().get());

    map.clear().get();

    assertEquals(0, map.size().get());
    assertTrue(map.isEmpty().get());
  }

  @Test
  void commitTest() throws IOException {
    MapIO<Integer, Integer, PureFunction<Integer, Integer>> map =
      Map
        .configWithFunctions(intSerializer(), intSerializer())
        .init()
        .get();

    IntStream
      .rangeClosed(1, 1000)
      .forEach(
        integer ->
          map.put(integer, integer).get()
      );

//    List<swaydb.Prepare<Integer, Integer, PureFunction<Integer, Integer>>> puts = Arrays.asList(swaydb.java.Prepare.put(1, 2));
//    map.commit(puts);

    map.clear().get();

    assertEquals(0, map.size().get());
    assertTrue(map.isEmpty().get());
  }


  @Test
  void comparatorTest() {

    Map.Config<Integer, Integer, Functions.Disabled, Functions.Disabled> config =
      Map.config(intSerializer(), intSerializer());

    Comparator<Integer> comparator =
      (left, right) -> left.compareTo(right) * -1;

    config.setComparator(IO.rightNeverException(comparator));

    assertTrue(config.getComparator().isRight());

    MapIO<Integer, Integer, Functions.Disabled> map =
      config
        .init()
        .get();

    assertDoesNotThrow(() -> map.put(1, 1).get());
    assertDoesNotThrow(() -> map.put(2, 2).get());

    List<Integer> integers = map
      .stream()
      .map(KeyVal::key)
      .materialize()
      .get();

    assertEquals(Arrays.asList(2, 1), integers);
  }

  @Test
  void createMapWithCustomSerializer() throws IOException {
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


    MapIO<Key, Value, ?> map =
      createMap(keySerializer, valueSerializer);


    assertDoesNotThrow(() -> map.put(key1, value1).get());
    assertDoesNotThrow(() -> map.put(key2, value2).get());

    List<Key> mapKeys =
      map
        .stream()
        .map(KeyVal::key)
        .materialize()
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
  void registerAndApplyFunction() {
    MapIO<Integer, Integer, PureFunction<Integer, Integer>> map =
      Map
        .configWithFunctions(intSerializer(), intSerializer())
        .init()
        .get();

    assertDoesNotThrow(() -> map.put(1, 1).get());
    assertEquals(map.get(1).get().get(), 1);

    PureFunction.OnKey<Integer, Integer> getKey =
      (key, deadline) ->
        swaydb.java.Apply.update(10, Optional.empty());

    map.registerFunction(getKey).get();
    map.applyFunction(1, getKey).get();

    Integer integer = map.get(1).get().get();
    System.out.println(integer);
  }
}
