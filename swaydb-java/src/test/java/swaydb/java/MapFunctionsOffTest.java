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
import swaydb.KeyVal;
import swaydb.Pair;
import swaydb.Prepare;
import swaydb.data.java.JavaEventually;
import swaydb.data.java.TestBase;
import swaydb.java.data.slice.Slice;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;

abstract class MapFunctionsOffTest extends TestBase implements JavaEventually {

  public abstract <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                                    Serializer<V> valueSerializer) throws IOException;

  public abstract <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                                    Serializer<V> valueSerializer,
                                                    KeyComparator<K> keyComparator) throws IOException;


  @Test
  void putTest() throws IOException {
    MapT<Integer, Integer, Void> map = createMap(intSerializer(), intSerializer());

    map.put(1, 1);
    map.put(2, 2);
    map.put(3, 3, Duration.ofSeconds(2));

    //list
    map.put(Arrays.asList(KeyVal.create(4, 4), KeyVal.create(5, 5)));
    //same with iterator
    map.put(Arrays.asList(KeyVal.create(6, 6), KeyVal.create(7, 7)).iterator());
    map.put(Stream.create(Arrays.asList(KeyVal.create(8, 8), KeyVal.create(9, 9))));

    map.commit(Arrays.asList(Prepare.put(10, 10), Prepare.put(11, 11)));

    HashMap<Integer, Integer> actualKeyValues = new HashMap<>();

    map
      .stream()
      .forEach(
        keyValue ->
          actualKeyValues.put(keyValue.key(), keyValue.value())
      );

    map
      .iterator()
      .forEachRemaining(
        keyValue ->
          actualKeyValues.put(keyValue.key(), keyValue.value())
      );

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
          assertTrue(map.contains(integer));
          assertTrue(map.mightContain(integer));
        }
      );

    assertEquals(11, actualKeyValues.size());
    assertEquals(expectedKeyValues, actualKeyValues);

    eventuallyInSeconds(3,
      () -> {
        boolean present = map.get(3).isPresent();
        assertFalse(present);
        return present;
      });

    map.delete();
  }

  @Test
  void removeTest() throws IOException {
    MapT<Integer, Integer, Void> map = createMap(intSerializer(), intSerializer());

    //put 100 key-values
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          map.put(integer, integer)
      );


    //they should exist.
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          assertEquals(integer, map.get(integer).get())
      );


    //remove 10 key-values one by one
    IntStream
      .rangeClosed(1, 10)
      .forEach(map::remove);

    //removed key-values do not exist.
    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          assertFalse(map.get(integer).isPresent())
      );

    //others exist
    IntStream
      .rangeClosed(11, 100)
      .forEach(
        integer ->
          assertEquals(integer, map.get(integer).get())
      );

    //remove range
    map.remove(11, 50);

    //range key-values do not exists.
    IntStream
      .rangeClosed(0, 50)
      .forEach(
        integer ->
          assertFalse(map.get(integer).isPresent())
      );

    //remove range
    map.commit(Stream.range(51, 100).map(Prepare::removeFromMap));

    //non exist
    IntStream
      .rangeClosed(0, 100)
      .forEach(
        integer ->
          assertFalse(map.get(integer).isPresent())
      );

    map.delete();
  }

  @Test
  void expireTest() throws IOException {
    MapT<Integer, Integer, Void> map = createMap(intSerializer(), intSerializer());

    Duration expireAfter = Duration.ofSeconds(2);

    //put and then expire
    map.put(1, 1);
    map.expire(1, expireAfter);

    //put expire
    map.put(2, 2, expireAfter);

    //put list and expire list
    map.put(Arrays.asList(KeyVal.create(3, 3), KeyVal.create(4, 4)));
    map.expire(Arrays.asList(Pair.create(3, expireAfter), Pair.create(4, expireAfter)).iterator());

    //put list and expire stream
    map.put(Arrays.asList(KeyVal.create(5, 5), KeyVal.create(6, 6)));
    map.expire(Stream.create(Arrays.asList(Pair.create(5, expireAfter), Pair.create(6, expireAfter))));

    map.commit(
      Arrays.asList(
        Prepare.put(7, 7),
        Prepare.put(8, 8),
        Prepare.expireFromMap(7, Duration.ofSeconds(2)),
        Prepare.expireFromMap(8, Duration.ofSeconds(2))
      )
    );

    assertEquals(8, map.stream().size());

    IntStream
      .rangeClosed(1, 8)
      .forEach(
        integer ->
          assertTrue(map.get(integer).isPresent())
      );

    eventuallyInSeconds(
      3,
      () -> {
        assertTrue(map.isEmpty());
        assertEquals(0, map.stream().size());

        IntStream
          .rangeClosed(1, 8)
          .forEach(
            integer ->
              assertFalse(map.get(integer).isPresent())
          );

        return true;
      });

    map.delete();
  }

  @Test
  void expireRangeShouldClearAllKeyValuesTest() throws IOException {
    MapT<Integer, Integer, Void> map = createMap(intSerializer(), intSerializer());

    int maxKeyValues = 10000;

    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(
        integer ->
          map.put(integer, integer)
      );

    //contains test
    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(
        integer -> {
          assertTrue(map.contains(integer));
          assertTrue(map.mightContain(integer));
        }
      );

    assertEquals(maxKeyValues, map.stream().size());

    //expire individually
    IntStream
      .rangeClosed(1, maxKeyValues / 2)
      .forEach(
        value ->
          map.expire(value, Duration.ofSeconds(1))
      );

    //expire range.
    map.expire(maxKeyValues / 2, maxKeyValues, Duration.ofSeconds(1));

    eventuallyInSeconds(
      2,
      () -> {
        assertEquals(0, map.stream().size());
        assertTrue(map.isEmpty());
        return true;
      }
    );

    map.delete();
  }


  @Test
  void updateTest() throws IOException {
    MapT<Integer, Integer, Void> map = createMap(intSerializer(), intSerializer());

    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          map.put(integer, integer)
      );

    IntStream
      .rangeClosed(1, 50)
      .forEach(
        integer ->
          map.update(integer, integer + 1)
      );

    Stream<KeyVal<Integer, Integer>> updateStream =
      Stream
        .create(IntStream.rangeClosed(51, 80).iterator())
        .map(
          integer ->
            KeyVal.create(integer, integer + 1)
        );

    //update via stream
    map.update(updateStream);

    //update via range.
    map.update(81, 90, 0);

    map.commit(Collections.singletonList(Prepare.update(91, 100, 0)));

    IntStream
      .rangeClosed(1, 80)
      .forEach(
        integer ->
          assertEquals(integer + 1, map.get(integer).get())
      );

    IntStream
      .rangeClosed(81, 100)
      .forEach(
        integer ->
          assertEquals(0, map.get(integer).get())
      );

    map.delete();
  }

  @Test
  void clearTest() throws IOException {
    MapT<Integer, Integer, Void> map = createMap(intSerializer(), intSerializer());

    IntStream
      .rangeClosed(1, 100000)
      .forEach(
        integer ->
          map.put(integer, integer)
      );

    assertEquals(100000, map.stream().size());

    map.clearKeyValues();

    assertEquals(0, map.stream().size());
    assertTrue(map.isEmpty());

    map.delete();
  }

  @Test
  void commitTest() throws IOException {
    MapT<Integer, Integer, Void> map = createMap(intSerializer(), intSerializer());

    //create a 100 key-values
    map.put(Stream.range(1, 100).map(KeyVal::create));

    map.commit(
      Arrays.asList(
        Prepare.put(1, 11),
        Prepare.put(2, 22),
        Prepare.put(10, 100, Duration.ofSeconds(3)),
        Prepare.removeFromMap(3, 3),
        Prepare.put(4, 44),
        Prepare.update(50, 1000),
        Prepare.update(51, 60, Integer.MAX_VALUE),
        Prepare.expireFromMap(2, Duration.ofSeconds(3)),
        Prepare.expireFromMap(61, 70, Duration.ofSeconds(3))
      )
    );

    //expected expiration to occur after 3 seconds. But do normal asserts first.

    assertEquals(11, map.get(1).get());
    assertEquals(22, map.get(2).get());
    assertEquals(100, map.get(10).get());
    assertFalse(map.get(3).isPresent());
    assertEquals(44, map.get(4).get());
    assertEquals(1000, map.get(50).get());

    IntStream
      .rangeClosed(51, 60)
      .forEach(
        integer ->
          assertEquals(Integer.MAX_VALUE, map.get(integer).get())
      );

    eventuallyInSeconds(
      4,
      () -> {
        assertFalse(map.get(2).isPresent());
        assertFalse(map.get(10).isPresent());
        IntStream
          .rangeClosed(61, 70)
          .forEach(
            integer ->
              assertFalse(map.get(integer).isPresent())
          );
        return false;
      }
    );

    map.delete();
  }

  @Test
  void comparatorTest() throws IOException {
    MapT<Integer, Integer, Void> map =
      createMap(intSerializer(), intSerializer(), (left, right) -> left.compareTo(right) * -1);

    assertDoesNotThrow(() -> map.put(1, 1));
    assertDoesNotThrow(() -> map.put(2, 2));

    List<Integer> integers = map
      .stream()
      .map(KeyVal::key)
      .materialize();

    assertEquals(Arrays.asList(2, 1), integers);

    map.delete();
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
      public Key read(Slice<Byte> slice) {
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
      public Value read(Slice<Byte> slice) {
        if (slice.get(0) == 1) {
          return value1;
        } else {
          return value2;
        }
      }
    };

    MapT<Key, Value, Void> map =
      createMap(keySerializer, valueSerializer);

    assertDoesNotThrow(() -> map.put(key1, value1));
    assertDoesNotThrow(() -> map.put(key2, value2));

    List<Key> mapKeys =
      map
        .stream()
        .map(KeyVal::key)
        .materialize();

    assertEquals(Arrays.asList(key1, key2), mapKeys);

    if (map instanceof swaydb.java.Map) {
      List<Integer> setKeys =
        ((swaydb.java.Map<Key, Value, Void>) map)
          .keys()
          .stream()
          .map(key -> key.key)
          .materialize();

      assertEquals(Arrays.asList(1, 2), setKeys);
    }

    map.delete();
  }
}
