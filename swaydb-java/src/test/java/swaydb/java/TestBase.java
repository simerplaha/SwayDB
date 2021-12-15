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

import swaydb.utils.OperatingSystem;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class TestBase {

  String projectTargetFolder = this.getClass().getClassLoader().getResource("").getPath();

  public Path testFileDirectory() {
    Path projectDirectory;
    if (OperatingSystem.isWindows()()) {
      projectDirectory = Paths.get(projectTargetFolder.substring(1)).getParent().getParent();
    } else {
      projectDirectory = Paths.get(projectTargetFolder).getParent().getParent();
    }

    Path path = projectDirectory.resolve("TEST_FILES");
    return path;
  }


  public Path testDirPath() {
    Path path = testFileDirectory().resolve(this.getClass().getSimpleName());
    return path;
  }

  public Path testDir() throws IOException {
    if (Files.notExists(testDirPath())) {
      return Files.createDirectories(testDirPath());
    } else {
      return testDirPath();
    }
  }

  public void deleteTestDir() throws IOException {
    Path folder = testDir();
    Files.walkFileTree(folder,
      new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult postVisitDirectory(
          Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(
          Path file, BasicFileAttributes attrs)
          throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }
      });
  }
}
