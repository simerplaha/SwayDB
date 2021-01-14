/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.java;

import org.junit.jupiter.api.AfterEach;
import swaydb.Apply;
import swaydb.PureFunction;
import swaydb.java.eventually.persistent.EventuallyPersistentMap;
import swaydb.java.eventually.persistent.EventuallyPersistentMultiMap;
import swaydb.java.memory.MemoryMap;
import swaydb.java.memory.MemoryMultiMap;
import swaydb.java.persistent.PersistentMap;
import swaydb.java.persistent.PersistentMultiMap;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.util.List;

/**
 * Maps
 */
class MemoryMapFunctionsOnTest extends MapFunctionsOnTest {

  @Override
  public boolean isPersistent() {
    return false;
  }

  public <K, V> Map<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                      Serializer<V> valueSerializer,
                                                                      List<PureFunction<K, V, Apply.Map<V>>> functions) {
    return
      MemoryMap
        .functionsOn(keySerializer, valueSerializer, functions)
        .get();
  }

  @Override
  public <K, V> Map<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                      Serializer<V> valueSerializer,
                                                                      List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                      KeyComparator<K> keyComparator) {
    return
      MemoryMap
        .functionsOn(keySerializer, valueSerializer, functions)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class PersistentMapFunctionsOnTest extends MapFunctionsOnTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  @Override
  public boolean isPersistent() {
    return true;
  }

  public <K, V> Map<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                      Serializer<V> valueSerializer,
                                                                      List<PureFunction<K, V, Apply.Map<V>>> functions) throws IOException {

    return
      PersistentMap
        .functionsOn(testDir(), keySerializer, valueSerializer, functions)
        .get();
  }

  @Override
  public <K, V> Map<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                      Serializer<V> valueSerializer,
                                                                      List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                      KeyComparator<K> keyComparator) throws IOException {
    return
      PersistentMap
        .functionsOn(testDir(), keySerializer, valueSerializer, functions)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class EventuallyPersistentMapFunctionsOnTest extends MapFunctionsOnTest {

  @Override
  public boolean isPersistent() {
    return true;
  }

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> Map<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                      Serializer<V> valueSerializer,
                                                                      List<PureFunction<K, V, Apply.Map<V>>> functions) throws IOException {

    return
      EventuallyPersistentMap
        .functionsOn(testDir(), keySerializer, valueSerializer, functions)
        .get();
  }

  @Override
  public <K, V> Map<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                      Serializer<V> valueSerializer,
                                                                      List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                      KeyComparator<K> keyComparator) throws IOException {
    return
      EventuallyPersistentMap
        .functionsOn(testDir(), keySerializer, valueSerializer, functions)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}


/**
 * MultiMap
 */
class MemoryMultiMapFunctionsOnTest extends MapFunctionsOnTest {

  @Override
  public boolean isPersistent() {
    return false;
  }

  public <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                       Serializer<V> valueSerializer,
                                                                       List<PureFunction<K, V, Apply.Map<V>>> functions) {
    return
      MemoryMultiMap
        .functionsOn(keySerializer, keySerializer, valueSerializer, functions)
        .get();
  }


  @Override
  public <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                       Serializer<V> valueSerializer,
                                                                       List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                       KeyComparator<K> keyComparator) {
    return
      MemoryMultiMap
        .functionsOn(keySerializer, keySerializer, valueSerializer, functions)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class PersistentMultiMapFunctionsOnTest extends MapFunctionsOnTest {

  @Override
  public boolean isPersistent() {
    return true;
  }

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                       Serializer<V> valueSerializer,
                                                                       List<PureFunction<K, V, Apply.Map<V>>> functions) throws IOException {

    return
      PersistentMultiMap
        .functionsOn(testDir(), keySerializer, keySerializer, valueSerializer, functions)
        .get();
  }

  @Override
  public <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                       Serializer<V> valueSerializer,
                                                                       List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                       KeyComparator<K> keyComparator) throws IOException {
    return
      PersistentMultiMap
        .functionsOn(testDir(), keySerializer, keySerializer, valueSerializer, functions)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class EventuallyPersistentMultiMapFunctionsOnTest extends MapFunctionsOnTest {

  @Override
  public boolean isPersistent() {
    return true;
  }

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                       Serializer<V> valueSerializer,
                                                                       List<PureFunction<K, V, Apply.Map<V>>> functions) throws IOException {

    return
      EventuallyPersistentMultiMap
        .functionsOn(testDir(), keySerializer, keySerializer, valueSerializer, functions)
        .get();
  }

  @Override
  public <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                       Serializer<V> valueSerializer,
                                                                       List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                       KeyComparator<K> keyComparator) throws IOException {
    return
      EventuallyPersistentMultiMap
        .functionsOn(testDir(), keySerializer, keySerializer, valueSerializer, functions)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}
