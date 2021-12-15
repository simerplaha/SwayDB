/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
