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

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.JsonSerialization;

/**
 * 文件系统操作抽象接口，为清单提交器生成任务清单提供所需的存储操作能力。
 * 具体实现可配置，对象存储实现可通过子类扩展以支持弹性提交操作。
 * 该API仅允许在清单提交器及其测试代码中使用，不得在外部模块调用。
 */
@InterfaceAudience.LimitedPrivate("mapreduce, object-stores")
@InterfaceStability.Unstable
public abstract class ManifestStoreOperations implements Closeable {

  /**
   * 绑定到目标文件系统，在实例化操作对象后由清单提交器调用。
   * @param fileSystem 目标文件系统
   * @param path 文件系统下的实际工作路径
   * @throws IOException 绑定过程中出现IO错误
   */
  public void bindToFileSystem(FileSystem fileSystem, Path path) throws IOException {

  }

  /**
   * 获取指定路径的文件状态，转发调用FileSystem.getFileStatus。
   * @param path 目标路径
   * @return 文件状态信息
   * @throws IOException 获取过程中出现IO错误
   */
  public abstract FileStatus getFileStatus(Path path) throws IOException;

  /**
   * 判断指定路径是否为文件，用于目录创建过程中的路径探测。
   * 实现允许存储系统优化探测逻辑，减少不必要的IO操作。
   * @param path 待探测路径
   * @return true如果路径存在且为文件
   * @throws IOException 除FileNotFoundException外的IO错误
   */
  public boolean isFile(Path path) throws IOException {
    try {
      return getFileStatus(path).isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * 删除指定路径，转发调用FileSystem.delete。
   * 如果方法返回无异常，则表示路径已不存在。
   * @param path 待删除路径
   * @param recursive 是否递归删除
   * @return true如果路径成功被删除
   * @throws IOException 删除过程中出现IO错误
   */
  public abstract boolean delete(Path path, boolean recursive)
      throws IOException;

  /**
   * 删除单个文件，默认调用delete(path, false)实现。
   * 如果方法返回无异常，则表示路径已不存在文件。
   * @param path 待删除文件路径
   * @return 删除操作结果
   * @throws IOException 删除过程中出现IO错误
   */
  public boolean deleteFile(Path path)
      throws IOException {
    return delete(path, false);
  }

  /**
   * 递归删除路径，默认调用delete(path, true)实现。
   * 如果方法返回无异常，则表示路径已不存在。
   * @param path 待删除路径
   * @return 删除操作结果
   * @throws IOException 删除过程中出现IO错误
   */
  public boolean deleteRecursive(Path path)
      throws IOException {
    return delete(path, true);
  }

  /**
   * 创建指定目录，转发调用FileSystem.mkdirs。
   * @param path 待创建目录路径
   * @return true如果目录成功创建
   * @throws IOException 创建过程中出现IO错误
   */
  public abstract boolean mkdirs(Path path) throws IOException;

  /**
   * 重命名文件，转发调用FileSystem.rename。
   * @param source 源文件路径
   * @param dest 目标路径，必须不存在
   * @return 重命名操作返回值
   * @throws IOException 重命名过程中出现IO错误
   */
  public abstract boolean renameFile(Path source, Path dest)
      throws IOException;

  /**
   * 重命名目录，默认调用renameFile实现。
   * @param source 源目录路径
   * @param dest 目标路径，必须不存在
   * @return true如果目录成功重命名
   * @throws IOException 重命名过程中出现IO错误
   */
  public boolean renameDir(Path source, Path dest)
      throws IOException {
    return renameFile(source, dest);
  }

  /**
   * 迭代列出目录下所有文件状态。
   * @param path 待列出目录路径
   * @return 文件状态迭代器
   * @throws IOException 列出过程中出现即时IO错误
   */
  public abstract RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException;

  /**
   * 从存储加载任务清单文件。
   * @param serializer JSON序列化器
   * @param st 包含路径信息的文件状态
   * @return 加载解析后的任务清单对象
   * @throws IOException 加载或解析过程中出现错误
   */
  public abstract TaskManifest loadTaskManifest(
      JsonSerialization<TaskManifest> serializer,
      FileStatus st) throws IOException;

  /**
   * 将清单数据保存到存储，直接创建文件不做重命名操作。
   * @param manifestData 清单数据或成功标记文件
   * @param path 初始保存的临时路径
   * @param overwrite 是否允许覆盖已有文件
   * @throws IOException 保存过程中出现IO错误
   */
  public abstract <T extends AbstractManifestData<T>> void save(
      T manifestData,
      Path path,
      boolean overwrite) throws IOException;

  /**
   * 执行msync内存与存储同步操作，不支持时会静默忽略错误。
   * @param path 需要同步的路径
   * @throws IOException 同步过程中出现IO错误
   */
  public void msync(Path path) throws IOException {

  }


  /**
   * 从文件状态中提取ETag，满足提取条件时返回有效ETag，否则返回空。
   * @param status 文件状态，可以为null或任意FileStatus子类
   * @return 有效ETag，或null/空字符串表示无可用ETag
   */
  public String getEtag(FileStatus status) {
    return ManifestCommitterSupport.getEtag(status);
  }

  /**
   * 判断存储系统是否在重命名操作后保留ETag。
   * 如果返回true，且源文件存在ETag，会用于验证重命名失败场景。
   * @param path 待探测路径
   * @return true表示ETag比较是有效的验证策略
   */
  public boolean storePreservesEtagsThroughRenames(Path path) {
    return false;
  }

  /**
   * 判断存储是否通过commitFile实现提供弹性重命名提交能力。
   * 如果返回true，会调用该方法完成文件提交操作。
   * @return true表示支持弹性提交
   */
  public boolean storeSupportsResilientCommit() {
    return false;
  }

  /**
   * 通过弹性API提交单个文件，必须完成从源到目标的重命名，失败则抛出异常。
   * 返回值表示是否触发了恢复操作。基础实现默认抛出不支持异常。
   * @param entry 待提交的文件条目，包含ETag信息
   * @return 提交操作结果
   * @throws IOException 提交过程中出现IO错误
   * @throws UnsupportedOperationException 不支持弹性提交时抛出
   */
  public CommitFileResult commitFile(FileEntry entry) throws IOException {
    throw new UnsupportedOperationException("Resilient commit not supported");
  }

  /**
   * commitFile操作的结果封装，仅用于标识是否触发了恢复操作，重命名失败必须通过异常抛出。
   */
  public static final class CommitFileResult {

    /** 是否触发了恢复操作 */
    private final boolean recovered;

    /** 等待IO容量的时间，可为空 */
    @Nullable
    private final Duration waitTime;

    /**
     * 从弹性提交结果构造CommitFileResult实例。
     * @param recovered 是否触发恢复操作
     * @param waitTime 等待IO容量的时长
     * @return 构造完成的结果对象
     */
    public static CommitFileResult fromResilientCommit(
        final boolean recovered,
        final Duration waitTime) {
      return new CommitFileResult(recovered, waitTime);
    }

    /**
     * 构造CommitFileResult实例。
     * @param recovered 是否触发恢复操作
     * @param waitTime 等待IO容量的时长，可为空
     */
    public CommitFileResult(final boolean recovered,
        @Nullable final Duration waitTime) {

      this.recovered = recovered;
      this.waitTime = waitTime;
    }

    /**
     * 获取是否触发了恢复操作。
     * @return true表示提交通过(基于ETag的)恢复机制成功完成
     */
    public boolean recovered() {
      return recovered;
    }

    @Nullable
    public Duration getWaitTime() {
      return waitTime;
    }
  }

}