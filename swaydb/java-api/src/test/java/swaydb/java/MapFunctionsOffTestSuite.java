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
import swaydb.java.eventually.persistent.EventuallyPersistentMap;
import swaydb.java.eventually.persistent.EventuallyPersistentMultiMap;
import swaydb.java.memory.MemoryMap;
import swaydb.java.memory.MemoryMultiMap;
import swaydb.java.persistent.PersistentMap;
import swaydb.java.persistent.PersistentMultiMap;
import swaydb.java.serializers.Serializer;

import java.io.IOException;

/**
 * Maps
 */
class MemoryMapFunctionsOffTest extends MapFunctionsOffTest {

  public <K, V> Map<K, V, Void> createMap(Serializer<K> keySerializer,
                                          Serializer<V> valueSerializer) {
    return
      MemoryMap
        .functionsOff(keySerializer, valueSerializer)
        .get();
  }

  @Override
  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer,
                                           KeyComparator<K> keyComparator) {
    return
      MemoryMap
        .functionsOff(keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class PersistentMapFunctionsOffTest extends MapFunctionsOffTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> Map<K, V, Void> createMap(Serializer<K> keySerializer,
                                          Serializer<V> valueSerializer) throws IOException {

    return
      PersistentMap
        .functionsOff(testDir(), keySerializer, valueSerializer)
        .get();
  }

  @Override
  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer,
                                           KeyComparator<K> keyComparator) throws IOException {
    return
      PersistentMap
        .functionsOff(testDir(), keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class EventuallyPersistentMapFunctionsOffTest extends MapFunctionsOffTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> Map<K, V, Void> createMap(Serializer<K> keySerializer,
                                          Serializer<V> valueSerializer) throws IOException {

    return
      EventuallyPersistentMap
        .functionsOff(testDir(), keySerializer, valueSerializer)
        .get();
  }

  @Override
  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer,
                                           KeyComparator<K> keyComparator) throws IOException {
    return
      EventuallyPersistentMap
        .functionsOff(testDir(), keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}


/**
 * MultiMap
 */
class MemoryMultiMapFunctionsOffTest extends MapFunctionsOffTest {

  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer) {
    return
      MemoryMultiMap
        .functionsOff(keySerializer, keySerializer, valueSerializer)
        .get();
  }


  @Override
  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer,
                                           KeyComparator<K> keyComparator) {
    return
      MemoryMultiMap
        .functionsOff(keySerializer, keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class PersistentMultiMapFunctionsOffTest extends MapFunctionsOffTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer) throws IOException {

    return
      PersistentMultiMap
        .functionsOff(testDir(), keySerializer, keySerializer, valueSerializer)
        .get();
  }

  @Override
  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer,
                                           KeyComparator<K> keyComparator) throws IOException {
    return
      PersistentMultiMap
        .functionsOff(testDir(), keySerializer, keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class EventuallyPersistentMultiMapFunctionsOffTest extends MapFunctionsOffTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer) throws IOException {

    return
      EventuallyPersistentMultiMap
        .functionsOff(testDir(), keySerializer, keySerializer, valueSerializer)
        .get();
  }

  @Override
  public <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                           Serializer<V> valueSerializer,
                                           KeyComparator<K> keyComparator) throws IOException {
    return
      EventuallyPersistentMultiMap
        .functionsOff(testDir(), keySerializer, keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}
