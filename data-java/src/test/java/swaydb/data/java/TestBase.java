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

package swaydb.data.java;

import swaydb.data.util.OperatingSystem;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class TestBase {

  String projectTargetFolder = this.getClass().getClassLoader().getResource("").getPath();

  public Path testFileDirectory() {
    Path projectDirectory;
    if (OperatingSystem.isWindows()) {
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
