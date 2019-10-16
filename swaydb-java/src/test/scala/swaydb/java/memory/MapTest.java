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
import swaydb.java.*;
import swaydb.java.data.slice.ByteSlice;
import swaydb.java.data.util.KeyVal;
import swaydb.java.data.util.Pair;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;


class MemoryMapFunctionsDisabledTest extends MapTest {

  public <K, V> MapIO<K, V, PureFunction.VoidM<K, V>> createMap(Serializer<K> keySerializer,
                                                                Serializer<V> valueSerializer) {
    MapIO<K, V, PureFunction.VoidM<K, V>> map =
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

  public <K, V> MapIO<K, V, PureFunction.VoidM<K, V>> createMap(Serializer<K> keySerializer,
                                                                Serializer<V> valueSerializer) throws IOException {
    MapIO<K, V, PureFunction.VoidM<K, V>> map =
      swaydb.java.persistent.Map
        .config(testDir(), keySerializer, valueSerializer)
        .init()
        .get();

    return map;
  }
}

abstract class MapTest extends TestBase implements JavaEventually {

  public abstract <K, V> MapIO<K, V, PureFunction.VoidM<K, V>> createMap(Serializer<K> keySerializer,
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

    map.commit(Arrays.asList(Prepare.putInMap(10, 10), Prepare.putInMap(11, 11))).get();

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
      .rangeClosed(1, 11)
      .forEach(
        integer ->
          expectedKeyValues.put(integer, integer)
      );

    //contains test
    IntStream
      .rangeClosed(1, 11)
      .forEach(
        integer -> {
          assertTrue(map.contains(integer).get());
          assertTrue(map.mightContain(integer).get());
        }
      );

    assertEquals(11, actualKeyValues.size());
    assertEquals(expectedKeyValues, actualKeyValues);

    eventuallyInSeconds(3,
      () -> {
        boolean present = map.get(3).get().isPresent();
        assertFalse(present);
        return present;
      });
  }

  @Test
  void removeTest() throws IOException {
    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map = createMap(intSerializer(), intSerializer());

    //put 100 key-values
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          map.put(integer, integer).get()
      );


    //they should exist.
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          assertEquals(integer, map.get(integer).get().get())
      );


    //remove 10 key-values one by one
    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          map.remove(integer).get()
      );

    //removed key-values do not exist.
    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          assertFalse(map.get(integer).get().isPresent())
      );

    //others exist
    IntStream
      .rangeClosed(11, 100)
      .forEach(
        integer ->
          assertEquals(integer, map.get(integer).get().get())
      );

    //remove range
    map.remove(11, 100).get();

    //non exist
    IntStream
      .rangeClosed(0, 100)
      .forEach(
        integer ->
          assertFalse(map.get(integer).get().isPresent())
      );
  }

  @Test
  void expireTest() throws IOException {
    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map = createMap(intSerializer(), intSerializer());

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

    map.commit(
      Arrays.asList(
        Prepare.putInMap(7, 7),
        Prepare.putInMap(8, 8),
        Prepare.expireFromMap(7, Duration.ofSeconds(2)),
        Prepare.expireFromMap(8, Duration.ofSeconds(2))
      )
    ).get();

    assertEquals(8, map.size().get());

    IntStream
      .rangeClosed(1, 8)
      .forEach(
        integer ->
          assertTrue(map.get(integer).get().isPresent())
      );

    eventuallyInSeconds(
      3,
      () -> {
        assertTrue(map.isEmpty().get());
        assertEquals(0, map.size().get());

        IntStream
          .rangeClosed(1, 8)
          .forEach(
            integer ->
              assertFalse(map.get(integer).get().isPresent())
          );

        return true;
      });
  }

  @Test
  void expireRangeShouldClearAllKeyValuesTest() throws IOException {
    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map = createMap(intSerializer(), intSerializer());

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
      () -> {
        assertEquals(0, map.size().get());
        assertTrue(map.isEmpty().get());
        return true;
      }
    );
  }


  @Test
  void updateTest() throws IOException {
    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map = createMap(intSerializer(), intSerializer());

    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          map.put(integer, integer).get()
      );

    IntStream
      .rangeClosed(1, 50)
      .forEach(
        integer ->
          map.update(integer, integer + 1).get()
      );

    StreamIO<KeyVal<Integer, Integer>> updateStream =
      Stream
        .create(IntStream.rangeClosed(51, 80).iterator())
        .map(
          integer ->
            KeyVal.create(integer, integer + 1)
        );

    //update via stream
    map.update(updateStream).get();

    //update via range.
    map.update(81, 90, 0).get();

    map.commit(Collections.singletonList(Prepare.updateInMap(91, 100, 0))).get();

    IntStream
      .rangeClosed(1, 80)
      .forEach(
        integer ->
          assertEquals(integer + 1, map.get(integer).get().get())
      );

    IntStream
      .rangeClosed(81, 100)
      .forEach(
        integer ->
          assertEquals(0, map.get(integer).get().get())
      );
  }

  @Test
  void clearTest() throws IOException {
    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map = createMap(intSerializer(), intSerializer());

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
    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map = createMap(intSerializer(), intSerializer());

    final Iterator<Prepare.PutInMap<Integer, Integer, Void>> putStream = null;

//    List<Prepare.Map<Integer, Integer, Void>> puts = Arrays.asList(Prepare.putInMap(1, 1));


//    map.commit(putStream).get();
//
//    List<Prepare.Map<Integer, Integer, PureFunction<Integer, Integer, Return.Map<Integer>>>> puts =
//      Arrays.asList(
//        Prepare.putInMap(1, 2),
//        Prepare.applyFunctionInMap(2, function)
//      );
//
//    map.commit(puts).get();
//
//    assertEquals(2, map.get(1).get().get());
//    assertEquals(10, map.get(2).get().get());
  }

  @Test
  void comparatorTest() {

    Map.Config<Integer, Integer, PureFunction.VoidM<Integer, Integer>, Void> config =
      Map.config(intSerializer(), intSerializer());

    Comparator<Integer> comparator =
      (left, right) -> left.compareTo(right) * -1;

    config.setComparator(IO.rightNeverException(comparator));

    assertTrue(config.getComparator().isRight());

    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map =
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


    MapIO<Key, Value, PureFunction.VoidM<Key, Value>> map =
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
    MapIO<Integer, Integer, PureFunction<Integer, Integer, Return.Map<Integer>>> map =
      Map
        .configWithFunctions(intSerializer(), intSerializer())
        .init()
        .get();

    assertDoesNotThrow(() -> map.put(1, 1).get());
    assertEquals(map.get(1).get().get(), 1);

    PureFunction.OnKey<Integer, Integer, Return.Map<Integer>> getKey =
      (key, deadline) ->
        Return.update(10);

    PureFunction.OnValue<Integer, Integer, Return.Map<Integer>> onValue =
      value ->
        Return.update(value + 1);

    map.registerFunction(getKey).get();
    map.registerFunction(onValue).get();
    map.applyFunction(1, getKey).get();

    Integer integer = map.get(1).get().get();
    System.out.println(integer);
  }
}
