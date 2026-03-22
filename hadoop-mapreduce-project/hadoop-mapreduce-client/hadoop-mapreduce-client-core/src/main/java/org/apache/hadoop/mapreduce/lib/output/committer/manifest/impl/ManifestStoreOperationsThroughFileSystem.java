// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.JsonSerialization;

/**
 * 文件层面板清单提交器的清单存储操作实现，基于标准Hadoop FileSystem API实现。
 * 该类在ABFS模块中存在子类实现，添加了弹性提交方法。
 */
@InterfaceAudience.LimitedPrivate("mapreduce, object-stores")
@InterfaceStability.Unstable
public class ManifestStoreOperationsThroughFileSystem extends ManifestStoreOperations {

  /**
   * 操作使用的文件系统，在{@link #bindToFileSystem(FileSystem, Path)}中绑定。
   */
  private FileSystem fileSystem;

  /**
   * 标记FileSystem.msync()调用是否因不支持而失败。
   * 如果标记为true，后续调用{@link #msync(Path)}将不再尝试执行。
   */
  private boolean msyncUnsupported = false;

  /**
   * 直接构造函数，指定操作使用的文件系统。
   * @param fileSystem 要操作的文件系统
   */
  public ManifestStoreOperationsThroughFileSystem(final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  /**
   * 用于反射绑定的空构造函数。
   */
  public ManifestStoreOperationsThroughFileSystem() {
  }

  @Override
  public void close() throws IOException {
    /* no-op; FS is assumed to be shared. */

  }

  /**
   * 获取当前操作使用的文件系统。
   * @return 当前绑定的文件系统，绑定前返回null
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public void bindToFileSystem(FileSystem filesystem, Path path) throws IOException {
    fileSystem = filesystem;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return fileSystem.getFileStatus(path);
  }

  /**
   * 使用FileSystem.isFile方法判断路径是否为文件，允许存储系统优化探测逻辑。
   * @param path 要探测的路径
   * @return 如果路径对应文件则返回true
   * @throws IOException IO操作失败
   */
  @SuppressWarnings("deprecation")
  @Override
  public boolean isFile(Path path) throws IOException {
    return fileSystem.isFile(path);
  }

  @Override
  public boolean delete(Path path, boolean recursive)
      throws IOException {
    return fileSystem.delete(path, recursive);
  }

  @Override
  public boolean deleteRecursive(final Path path) throws IOException {
    return fileSystem.delete(path, true);
  }

  @Override
  public boolean mkdirs(Path path)
      throws IOException {
    return fileSystem.mkdirs(path);
  }

  @Override
  public boolean renameFile(Path source, Path dest)
      throws IOException {
    return fileSystem.rename(source, dest);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException {
    return fileSystem.listStatusIterator(path);
  }

  @Override
  public TaskManifest loadTaskManifest(
      JsonSerialization<TaskManifest> serializer,
      FileStatus st) throws IOException {
    return TaskManifest.load(serializer, fileSystem, st.getPath(), st);
  }

  @Override
  public <T extends AbstractManifestData<T>> void save(
      final T manifestData,
      final Path path,
      final boolean overwrite) throws IOException {
    manifestData.save(fileSystem, path, overwrite);
  }

  /**
   * 探测文件系统是否支持重命名保留ETag。
   * @param path 要探测的路径
   * @return 如果文件系统声明重命名后保留ETag则返回true
   */
  @Override
  public boolean storePreservesEtagsThroughRenames(Path path) {
    try {
      return fileSystem.hasPathCapability(path,
          CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME);
    } catch (IOException ignored) {
      return false;
    }
  }

  /**
   * 调用FileSystem的msync方法同步客户端元数据缓存，捕获并处理不支持该操作的异常。
   * 该操作用于保证HDFS-HA部署场景下客户端元数据缓存同步。
   * 大多数文件系统不支持该操作，首次调用失败后会设置标记，禁止后续重复尝试。
   * @param path 要同步的路径
   * @throws IOException 同步操作失败时抛出
   */
  @Override
  public void msync(Path path) throws IOException {
    // 如果已经确认不支持msync，直接返回
    if (msyncUnsupported) {
      return;
    }
    // 标准化路径，确保路径格式正确
    fileSystem.makeQualified(path);
    try {
      fileSystem.msync();
    } catch (UnsupportedOperationException ignored) {
      // 捕获不支持异常，标记为不支持，避免后续重复尝试
      msyncUnsupported = true;
    }
  }

}