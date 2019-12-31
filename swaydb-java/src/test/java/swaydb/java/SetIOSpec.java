/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.java;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import swaydb.data.java.JavaEventually;
import swaydb.data.java.TestBase;
import swaydb.java.data.slice.ByteSlice;
import swaydb.java.Pair;
import swaydb.java.memory.Set;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;


class MemorySetTest extends SetTest {

  public <K> SetIO<K, PureFunction.VoidS<K>> createSet(Serializer<K> keySerializer) {
    SetIO<K, PureFunction.VoidS<K>> map =
      swaydb.java.memory.Set
        .config(keySerializer)
        .init()
        .get();

    return map;
  }
}

class PersistentSetTest extends SetTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> SetIO<K, PureFunction.VoidS<K>> createSet(Serializer<K> keySerializer) throws IOException {
    SetIO<K, PureFunction.VoidS<K>> map =
      swaydb.java.persistent.Set
        .config(testDir(), keySerializer)
        .init()
        .get();

    return map;
  }
}

abstract class SetTest extends TestBase implements JavaEventually {

  public abstract <K> SetIO<K, PureFunction.VoidS<K>> createSet(Serializer<K> keySerializer) throws IOException;

  @Test
  void addTest() throws IOException {
    SetIO<Integer, PureFunction.VoidS<Integer>> set = createSet(intSerializer());

    set.add(1).get();
    set.add(2).get();
    set.add(3, Duration.ofSeconds(2)).get();
    //list
    set.add(Arrays.asList(4, 5)).get();
    //same with iterator
    set.add(Arrays.asList(6, 7).iterator()).get();
    set.add(Stream.create(Arrays.asList(8, 9))).get();

    set.commit(Arrays.asList(Prepare.addToSet(10), Prepare.addToSet(11))).get();

    HashSet<Integer> actualKeyValues = new HashSet<>();

    set
      .forEach(actualKeyValues::add)
      .materialize()
      .get();

    HashSet<Integer> expectedKeyValues = new HashSet<>();

    IntStream
      .rangeClosed(1, 11)
      .forEach(expectedKeyValues::add);

    //contains test
    IntStream
      .rangeClosed(1, 11)
      .forEach(
        integer -> {
          assertTrue(set.contains(integer).get());
          assertTrue(set.mightContain(integer).get());
        }
      );

    assertEquals(11, actualKeyValues.size());
    assertEquals(expectedKeyValues, actualKeyValues);

    eventuallyInSeconds(3,
      () -> {
        boolean present = set.get(3).get().isPresent();
        assertFalse(present);
        return present;
      });
  }

  @Test
  void removeTest() throws IOException {
    SetIO<Integer, PureFunction.VoidS<Integer>> set = createSet(intSerializer());

    //add 100 key-values
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          set.add(integer).get()
      );


    //they should exist.
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          assertEquals(integer, set.get(integer).get().get())
      );


    //remove 10 key-values one by one
    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          set.remove(integer).get()
      );

    //removed key-values do not exist.
    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          assertFalse(set.get(integer).get().isPresent())
      );

    //others exist
    IntStream
      .rangeClosed(11, 100)
      .forEach(
        integer ->
          assertEquals(integer, set.get(integer).get().get())
      );

    //remove range
    set.remove(11, 50).get();

    //range key-values do not exists.
    IntStream
      .rangeClosed(0, 50)
      .forEach(
        integer ->
          assertFalse(set.get(integer).get().isPresent())
      );

    //remove range
    set.commit(Stream.range(51, 100).map(Prepare::removeFromSet)).get();

    //non exist
    IntStream
      .rangeClosed(0, 100)
      .forEach(
        integer ->
          assertFalse(set.get(integer).get().isPresent())
      );
  }

  @Test
  void expireTest() throws IOException {
    SetIO<Integer, PureFunction.VoidS<Integer>> set = createSet(intSerializer());

    Duration expireAfter = Duration.ofSeconds(2);

    //add and then expire
    set.add(1).get();
    set.expire(1, expireAfter).get();

    //add expire
    set.add(2, expireAfter).get();

    //add list and expire list
    set.add(Arrays.asList(3, 4)).get();
    set.expire(Arrays.asList(new Pair<>(3, expireAfter), new Pair<>(4, expireAfter)).iterator()).get();

    //add list and expire stream
    set.add(Arrays.asList(5, 6)).get();
    set.expire(Stream.create(Arrays.asList(new Pair<>(5, expireAfter), new Pair<>(6, expireAfter)))).get();

    set.commit(
      Arrays.asList(
        Prepare.addToSet(7),
        Prepare.addToSet(8),
        Prepare.expireFromSet(7, Duration.ofSeconds(2)),
        Prepare.expireFromSet(8, Duration.ofSeconds(2))
      )
    ).get();

    assertEquals(8, set.size().get());

    IntStream
      .rangeClosed(1, 8)
      .forEach(
        integer ->
          assertTrue(set.get(integer).get().isPresent())
      );

    eventuallyInSeconds(
      3,
      () -> {
        assertTrue(set.isEmpty().get());
        assertEquals(0, set.size().get());

        IntStream
          .rangeClosed(1, 8)
          .forEach(
            integer ->
              assertFalse(set.get(integer).get().isPresent())
          );

        return true;
      });
  }

  @Test
  void expireRangeShouldClearAllKeyValuesTest() throws IOException {
    SetIO<Integer, PureFunction.VoidS<Integer>> set = createSet(intSerializer());

    int maxKeyValues = 10000;

    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(
        integer ->
          set.add(integer).get()
      );

    //contains test
    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(
        integer -> {
          assertTrue(set.contains(integer).get());
          assertTrue(set.mightContain(integer).get());
        }
      );

    assertEquals(maxKeyValues, set.size().get());

    //expire individually
    IntStream
      .rangeClosed(1, maxKeyValues / 2)
      .forEach(
        value ->
          set.expire(value, Duration.ofSeconds(1))
      );

    //expire range.
    set.expire(maxKeyValues / 2, maxKeyValues, Duration.ofSeconds(1)).get();

    eventuallyInSeconds(
      2,
      () -> {
        assertEquals(0, set.size().get());
        assertTrue(set.isEmpty().get());
        return true;
      }
    );
  }

  @Test
  void clearTest() throws IOException {
    SetIO<Integer, PureFunction.VoidS<Integer>> set = createSet(intSerializer());

    IntStream
      .rangeClosed(1, 100000)
      .forEach(
        integer ->
          set.add(integer).get()
      );

    assertEquals(100000, set.size().get());

    set.clear().get();

    assertEquals(0, set.size().get());
    assertTrue(set.isEmpty().get());
  }

  @Test
  void commitTest() throws IOException {
    SetIO<Integer, PureFunction.VoidS<Integer>> set = createSet(intSerializer());

    //create a 100 key-values
    set.add(Stream.range(1, 100)).get();

    set.commit(
      Arrays.asList(
        Prepare.addToSet(1),
        Prepare.addToSet(2),
        Prepare.addToSet(10, Duration.ofSeconds(3)),
        Prepare.removeFromSet(3, 3),
        Prepare.expireFromSet(2, Duration.ofSeconds(3)),
        Prepare.expireFromSet(61, 70, Duration.ofSeconds(3))
      )
    ).get();

    //expected expiration to occur after 3 seconds. But do normal asserts first.

    assertEquals(1, set.get(1).get().get());
    assertEquals(2, set.get(2).get().get());
    assertEquals(10, set.get(10).get().get());
    assertFalse(set.get(3).get().isPresent());

    eventuallyInSeconds(
      4,
      () -> {
        assertFalse(set.get(2).get().isPresent());
        assertFalse(set.get(10).get().isPresent());
        IntStream
          .rangeClosed(61, 70)
          .forEach(
            integer ->
              assertFalse(set.get(integer).get().isPresent())
          );
        return false;
      }
    );
  }

  @Test
  void comparatorTest() {

    Set.Config<Integer, PureFunction.VoidS<Integer>, Void> config =
      Set.config(intSerializer());

    Comparator<Integer> comparator =
      (left, right) -> left.compareTo(right) * -1;

    config.setComparator(IO.rightNeverException(comparator));

    assertTrue(config.getComparator().isRight());

    SetIO<Integer, PureFunction.VoidS<Integer>> set =
      config
        .init()
        .get();

    assertDoesNotThrow(() -> set.add(1).get());
    assertDoesNotThrow(() -> set.add(2).get());

    List<Integer> integers =
      set
        .stream()
        .materialize()
        .get();

    assertEquals(Arrays.asList(2, 1), integers);
  }

  @Test
  void createSetWithCustomSerializer() throws IOException {
    class Key {
      Integer key;

      Key setKey(Integer key) {
        this.key = key;
        return this;
      }
    }

    Key key1 = new Key().setKey(1);
    Key key2 = new Key().setKey(2);

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

    SetIO<Key, PureFunction.VoidS<Key>> set =
      createSet(keySerializer);

    assertDoesNotThrow(() -> set.add(key1).get());
    assertDoesNotThrow(() -> set.add(key2).get());

    List<Key> mapKeys =
      set
        .stream()
        .materialize()
        .get();

    assertEquals(Arrays.asList(key1, key2), mapKeys);

    List<Integer> setKeys =
      set
        .stream()
        .map(key -> key.key)
        .materialize()
        .get();

    assertEquals(Arrays.asList(1, 2), setKeys);
  }


  @Test
  void registerAndApplyFunction() {
    SetIO<Integer, PureFunction.OnKey<Integer, Void, Return.Set<Void>>> set = Set
      .configWithFunctions(intSerializer())
      .init()
      .get();

    set.add(Stream.range(1, 100)).get();

    PureFunction.OnKey<Integer, Void, Return.Set<Void>> expire =
      (key, deadline) ->
        Return.expire(Duration.ZERO);

    //does not compile
//    PureFunction.OnValue<Integer, Integer, Return.Set<Integer>> incrementBy1 = null;
//    set.registerFunction(incrementBy1).get();

    //does not compile
//    PureFunction.OnKeyValue<Integer, Integer, Return.Set<Integer>> removeMod0OrIncrementBy1 = null;
//    set.registerFunction(removeMod0OrIncrementBy1).get();

    //this will not compile since the return type specified is a Set - expected!
//    PureFunction.OnValue<Integer, Integer, Return.Set<Integer>> set = null;
//    set.registerFunction(set).get();

    set.registerFunction(expire).get();

    set.applyFunction(1, 100, expire);

    assertTrue(set.isEmpty().get());

  }
}
