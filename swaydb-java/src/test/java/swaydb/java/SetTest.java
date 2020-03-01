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
 */

package swaydb.java;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import swaydb.Pair;
import swaydb.data.java.JavaEventually;
import swaydb.data.java.TestBase;
import swaydb.java.data.slice.ByteSlice;
import swaydb.java.memory.SetBuilder;
import swaydb.java.serializers.Serializer;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static swaydb.java.serializers.Default.intSerializer;


class MemorySetTest extends SetTest {

  public <K> Set<K, Void> createSet(Serializer<K> keySerializer) {
    Set<K, Void> set =
      swaydb.java.memory.SetBuilder
        .createFunctionsDisabled(keySerializer)
        .build();

    return set;
  }
}

class PersistentSetTest extends SetTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K> Set<K, Void> createSet(Serializer<K> keySerializer) throws IOException {
    Set<K, Void> set =
      swaydb.java.persistent.SetBuilder
        .createFunctionsDisabled(testDir(), keySerializer)
        .build();

    return set;
  }
}

abstract class SetTest extends TestBase implements JavaEventually {

  public abstract <K> Set<K, Void> createSet(Serializer<K> keySerializer) throws IOException;

  @Test
  void addTest() throws IOException {
    Set<Integer, Void> set = createSet(intSerializer());

    set.add(1);
    set.add(2);
    set.add(3, Duration.ofSeconds(2));
    //list
    set.add(Arrays.asList(4, 5));
    //same with iterator
    set.add(Arrays.asList(6, 7).iterator());
    set.add(Stream.create(Arrays.asList(8, 9)));

    set.commit(Arrays.asList(Prepare.addToSet(10), Prepare.addToSet(11)));

    HashSet<Integer> actualKeyValues = new HashSet<>();

    set
      .stream()
      .forEach(actualKeyValues::add);

    HashSet<Integer> expectedKeyValues = new HashSet<>();

    IntStream
      .rangeClosed(1, 11)
      .forEach(expectedKeyValues::add);

    //contains test
    IntStream
      .rangeClosed(1, 11)
      .forEach(
        integer -> {
          assertTrue(set.contains(integer));
          assertTrue(set.mightContain(integer));
        }
      );

    assertEquals(11, actualKeyValues.size());
    assertEquals(expectedKeyValues, actualKeyValues);

    eventuallyInSeconds(3,
      () -> {
        boolean present = set.get(3).isPresent();
        assertFalse(present);
        return present;
      });
  }

  @Test
  void removeTest() throws IOException {
    Set<Integer, Void> set = createSet(intSerializer());

    //add 100 key-values
    IntStream
      .rangeClosed(1, 100)
      .forEach(set::add);


    //they should exist.
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          assertEquals(integer, set.get(integer).get())
      );


    //remove 10 key-values one by one
    IntStream
      .rangeClosed(1, 10)
      .forEach(set::remove);

    //removed key-values do not exist.
    IntStream
      .rangeClosed(1, 10)
      .forEach(
        integer ->
          assertFalse(set.get(integer).isPresent())
      );

    //others exist
    IntStream
      .rangeClosed(11, 100)
      .forEach(
        integer ->
          assertEquals(integer, set.get(integer).get())
      );

    //remove range
    set.remove(11, 50);

    //range key-values do not exists.
    IntStream
      .rangeClosed(0, 50)
      .forEach(
        integer ->
          assertFalse(set.get(integer).isPresent())
      );

    //remove range
    set.commit(Stream.range(51, 100).map(Prepare::removeFromSet));

    //non exist
    IntStream
      .rangeClosed(0, 100)
      .forEach(
        integer ->
          assertFalse(set.get(integer).isPresent())
      );
  }

  @Test
  void expireTest() throws IOException {
    Set<Integer, Void> set = createSet(intSerializer());

    Duration expireAfter = Duration.ofSeconds(2);

    //add and then expire
    set.add(1);
    set.expire(1, expireAfter);

    //add expire
    set.add(2, expireAfter);

    //add list and expire list
    set.add(Arrays.asList(3, 4));
    set.expire(Arrays.asList(new Pair<>(3, expireAfter), new Pair<>(4, expireAfter)).iterator());

    //add list and expire stream
    set.add(Arrays.asList(5, 6));
    set.expire(Stream.create(Arrays.asList(new Pair<>(5, expireAfter), new Pair<>(6, expireAfter))));

    set.commit(
      Arrays.asList(
        Prepare.addToSet(7),
        Prepare.addToSet(8),
        Prepare.expireFromSet(7, Duration.ofSeconds(2)),
        Prepare.expireFromSet(8, Duration.ofSeconds(2))
      )
    );

    assertEquals(8, set.stream().size());

    IntStream
      .rangeClosed(1, 8)
      .forEach(
        integer ->
          assertTrue(set.get(integer).isPresent())
      );

    eventuallyInSeconds(
      3,
      () -> {
        assertTrue(set.isEmpty());
        assertEquals(0, set.stream().size());

        IntStream
          .rangeClosed(1, 8)
          .forEach(
            integer ->
              assertFalse(set.get(integer).isPresent())
          );

        return true;
      });
  }

  @Test
  void expireRangeShouldClearAllKeyValuesTest() throws IOException {
    Set<Integer, Void> set = createSet(intSerializer());

    int maxKeyValues = 10000;

    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(set::add);

    //contains test
    IntStream
      .rangeClosed(1, maxKeyValues)
      .forEach(
        integer -> {
          assertTrue(set.contains(integer));
          assertTrue(set.mightContain(integer));
        }
      );

    assertEquals(maxKeyValues, set.stream().size());

    //expire individually
    IntStream
      .rangeClosed(1, maxKeyValues / 2)
      .forEach(
        value ->
          set.expire(value, Duration.ofSeconds(1))
      );

    //expire range.
    set.expire(maxKeyValues / 2, maxKeyValues, Duration.ofSeconds(1));

    eventuallyInSeconds(
      2,
      () -> {
        assertEquals(0, set.stream().size());
        assertTrue(set.isEmpty());
        return true;
      }
    );
  }

  @Test
  void clearTest() throws IOException {
    Set<Integer, Void> set = createSet(intSerializer());

    IntStream
      .rangeClosed(1, 100000)
      .forEach(set::add);

    assertEquals(100000, set.stream().size());

    set.clear();

    assertEquals(0, set.stream().size());
    assertTrue(set.isEmpty());
  }

  @Test
  void commitTest() throws IOException {
    Set<Integer, Void> set = createSet(intSerializer());

    //create a 100 key-values
    set.add(Stream.range(1, 100));

    set.commit(
      Arrays.asList(
        Prepare.addToSet(1),
        Prepare.addToSet(2),
        Prepare.addToSet(10, Duration.ofSeconds(3)),
        Prepare.removeFromSet(3, 3),
        Prepare.expireFromSet(2, Duration.ofSeconds(3)),
        Prepare.expireFromSet(61, 70, Duration.ofSeconds(3))
      )
    );

    //expected expiration to occur after 3 seconds. But do normal asserts first.

    assertEquals(1, set.get(1).get());
    assertEquals(2, set.get(2).get());
    assertEquals(10, set.get(10).get());
    assertFalse(set.get(3).isPresent());

    eventuallyInSeconds(
      4,
      () -> {
        assertFalse(set.get(2).isPresent());
        assertFalse(set.get(10).isPresent());
        IntStream
          .rangeClosed(61, 70)
          .forEach(
            integer ->
              assertFalse(set.get(integer).isPresent())
          );
        return false;
      }
    );
  }

  @Test
  void comparatorTest() {
    Set<Integer, Void> set =
      SetBuilder
        .createFunctionsDisabled(intSerializer())
        .setTypedComparator((left, right) -> left.compareTo(right) * -1)
        .build();

    assertDoesNotThrow(() -> set.add(1));
    assertDoesNotThrow(() -> set.add(2));

    List<Integer> integers =
      set
        .stream()
        .materialize();

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

    Set<Key, Void> set = createSet(keySerializer);

    assertDoesNotThrow(() -> set.add(key1));
    assertDoesNotThrow(() -> set.add(key2));

    List<Key> mapKeys =
      set
        .stream()
        .materialize();

    assertEquals(Arrays.asList(key1, key2), mapKeys);

    List<Integer> setKeys =
      set
        .stream()
        .map(key -> key.key)
        .materialize();

    assertEquals(Arrays.asList(1, 2), setKeys);
  }


  @Test
  void registerAndApplyFunction() {
    SetBuilder.Builder<Integer, PureFunction.OnKey<Integer, Void, Return.Set<Void>>> builder =
      SetBuilder
        .createFunctionsEnabled(intSerializer());

    Set<Integer, PureFunction.OnKey<Integer, Void, Return.Set<Void>>> set =
      builder
        .build();

    set.add(Stream.range(1, 100));

    PureFunction.OnKey<Integer, Void, Return.Set<Void>> expire =
      (key, deadline) ->
        Return.expire(Duration.ZERO);

    //does not compile
//    PureFunction.OnValue<Integer, Integer, Return.Set<Integer>> incrementBy1 = null;
//    config.registerFunction(incrementBy1);

    //does not compile
//    PureFunction.OnKeyValue<Integer, Integer, Return.Set<Integer>> removeMod0OrIncrementBy1 = null;
//    config.registerFunction(removeMod0OrIncrementBy1);

    //this will not compile since the return type specified is a Set - expected!
//    PureFunction.OnValue<Integer, Integer, Return.Set<Integer>> set = null;
//    config.registerFunction(set);

    builder.registerFunction(expire);

    set.applyFunction(1, 100, expire);

    assertTrue(set.isEmpty());

  }

  /**
   * Key type used for test partialKeyOrderingSetTest
   */
  private static class MyKey implements Serializable {
    int id = 0;
    String string = "";


    public MyKey(int id, String string) {
      this.id = id;
      this.string = string;
    }

    @Override
    public String toString() {
      return "MyKey{" +
        "id=" + id +
        ", string='" + string + '\'' +
        '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MyKey myKey = (MyKey) o;
      return id == myKey.id &&
        Objects.equals(string, myKey.string);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, string);
    }
  }

//  /**
//   * Tests partially ordered keys.
//   */
//  @Test
//  void partialKeyOrderingSetTest() throws IOException {
//
//    //create a serialiser using ObjectOutputStream
//    Serializer<MyKey> serializer =
//      new Serializer<MyKey>() {
//        @Override
//        public byte[] write(MyKey data) {
//          try {
//            ByteArrayOutputStream bos = new ByteArrayOutputStream();
//            ObjectOutputStream oos = new ObjectOutputStream(bos);
//            oos.writeObject(data);
//            oos.flush();
//            return bos.toByteArray();
//          } catch (IOException e) {
//            throw new RuntimeException(e);
//          }
//        }
//
//        @Override
//        public MyKey read(ByteSlice slice) {
//          ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(slice.toByteBufferWrap().array(), slice.fromOffset(), slice.size());
//          try {
//            ObjectInputStream oos = new ObjectInputStream(byteArrayInputStream);
//            return (MyKey) oos.readObject();
//          } catch (IOException | ClassNotFoundException e) {
//            throw new RuntimeException(e);
//          }
//        }
//      };
//
//    //partial key comparator
//    KeyComparator<MyKey> comparator =
//      new KeyComparator<MyKey>() {
//        @Override
//        public int compare(MyKey o1, MyKey o2) {
//          return Integer.compare(o1.id, o2.id);
//        }
//
//        @Override
//        public MyKey comparableKey(MyKey data) {
//          //since above compare is done only on id set the value of string to a static value..
//          return new MyKey(data.id, "");
//        }
//      };
//
//    //use a small map size so that Segments file gets generated for this this quickly.
//    int mapSize = 1000;
//
//    //memory set
//    SetBuilder.Builder<MyKey, Void> memoryConfig = SetBuilder.functionsDisabled(serializer);
//    memoryConfig.setComparator(IO.rightNeverException(comparator));
//    memoryConfig.setMapSize(mapSize);
//    Set<MyKey, Void> memorySet = memoryConfig.build();
//
//    //persistent Set
//    swaydb.java.persistent.SetBuilder.Builder<MyKey, Void> persistentConfig = swaydb.java.persistent.SetBuilder.functionsDisabled(testDir(), serializer);
//    persistentConfig.setComparator(IO.rightNeverException(comparator));
//    persistentConfig.setMapSize(mapSize);
//    Set<MyKey, Void> persistentSet = persistentConfig.build();
//
//    //create a slice to test for both maps
//    Slice<Set<MyKey, Void>> sets = Slice.create(2);
//    sets.add(memorySet);
//    sets.add(persistentSet);
//
//    sets.forEach(
//      set -> {
//        IntStream
//          .range(1, 2000)
//          .forEach(
//            integer ->
//              set.add(new MyKey(integer, "value" + integer))
//          );
//
//        IntStream
//          .range(1, 2000)
//          .forEach(
//            integer -> {
//              //here string in Key can be empty (partial key) but on get the entire key with string populated will be fetched.
//              Optional<MyKey> myKey = set.get(new MyKey(integer, ""));
//              assertTrue(myKey.isPresent());
//              assertEquals(new MyKey(integer, "value" + integer), myKey.get());
//            }
//          );
//      }
//    );
//  }
}
