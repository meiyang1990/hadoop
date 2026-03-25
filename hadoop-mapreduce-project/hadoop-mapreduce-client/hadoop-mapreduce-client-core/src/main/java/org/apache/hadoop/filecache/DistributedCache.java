// 这个文件已经全部加上中文注释
/**
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

package org.apache.hadoop.filecache;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 分布式缓存工具类，用于高效分发应用程序所需的大型只读文件到集群所有计算节点。
 *
 * <p><code>DistributedCache</code> 是MapReduce框架提供的缓存工具，用于缓存应用所需的文件
 * （文本文件、压缩包、JAR包等）。</p>
 *
 * <p>应用通过URI指定需要缓存的文件，分布式缓存假设这些文件已经存在于文件系统中，
 * 并且集群中所有节点都可以访问。</p>
 *
 * <p>框架会在节点执行任何任务之前，将所需文件拷贝到该工作节点，效率来自于每个作业
 * 只拷贝一次文件，并且支持在节点上自动解压归档文件。</p>
 *
 * <p>该类是MapReduce 1.x时代的API，已被标记为Deprecated，推荐使用新版本API。
 * 当前继承了新版本的实现保持向后兼容性。</p>
 * 
 * @see org.apache.hadoop.mapreduce.filecache.DistributedCache
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public class DistributedCache extends
    org.apache.hadoop.mapreduce.filecache.DistributedCache {
  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_FILES_SIZES}。
   */
  @Deprecated
  public static final String CACHE_FILES_SIZES =
      "mapred.cache.files.filesizes";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_ARCHIVES_SIZES}。
   */
  @Deprecated
  public static final String CACHE_ARCHIVES_SIZES =
    "mapred.cache.archives.filesizes";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_ARCHIVES_TIMESTAMPS}。
   */
  @Deprecated
  public static final String CACHE_ARCHIVES_TIMESTAMPS =
      "mapred.cache.archives.timestamps";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_FILE_TIMESTAMPS}。
   */
  @Deprecated
  public static final String CACHE_FILES_TIMESTAMPS =
      "mapred.cache.files.timestamps";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_ARCHIVES}。
   */
  @Deprecated
  public static final String CACHE_ARCHIVES = "mapred.cache.archives";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_FILES}。
   */
  @Deprecated
  public static final String CACHE_FILES = "mapred.cache.files";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_LOCALARCHIVES}。
   */
  @Deprecated
  public static final String CACHE_LOCALARCHIVES =
      "mapred.cache.localArchives";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_LOCALFILES}。
   */
  @Deprecated
  public static final String CACHE_LOCALFILES = "mapred.cache.localFiles";

  /**
   * 警告：这不是公开常量，仅为兼容MapReduce 1.x应用保留，MapReduce 2.x应用应使用{@link MRJobConfig#CACHE_SYMLINK}。
   */
  @Deprecated
  public static final String CACHE_SYMLINK = "mapred.create.symlink";

  /**
   * 将已本地化的归档文件添加到配置中，供分布式缓存内部使用。
   * @param conf 要修改的配置对象
   * @param str 逗号分隔的本地归档文件路径列表
   */
  @Deprecated
  public static void addLocalArchives(Configuration conf, String str) {
    String archives = conf.get(CACHE_LOCALARCHIVES);
    conf.set(CACHE_LOCALARCHIVES, archives == null ? str
        : archives + "," + str);
  }

  /**
   * 将已本地化的文件添加到配置中，供分布式缓存内部使用。
   * @param conf 要修改的配置对象
   * @param str 逗号分隔的本地文件路径列表
   */
  @Deprecated
  public static void addLocalFiles(Configuration conf, String str) {
    String files = conf.get(CACHE_LOCALFILES);
    conf.set(CACHE_LOCALFILES, files == null ? str
        : files + "," + str);
  }

  /**
   * 在指定工作目录为缓存目录中的所有文件创建符号链接，当前版本已不支持禁用符号链接，此方法为空操作。
   *
   * @param conf 配置对象
   * @param jobCacheDir 缓存文件所在的目标目录
   * @param workDir 需要创建符号链接的工作目录
   * @throws IOException 不会抛出异常
   * @deprecated 仅MapReduce框架内部使用，请改用DistributedCacheManager
   */
  @Deprecated
  public static void createAllSymlink(
      Configuration conf, File jobCacheDir, File workDir)
    throws IOException{
    // Do nothing
  }

  /**
   * 获取HDFS上指定缓存文件的FileStatus对象，供MapReduce内部使用。
   * @param conf 配置对象
   * @param cache 缓存文件的URI
   * @return HDFS上该缓存文件的FileStatus
   * @throws IOException 获取文件状态失败时抛出
   */
  @Deprecated
  public static FileStatus getFileStatus(Configuration conf, URI cache)
    throws IOException {
    FileSystem fileSystem = FileSystem.get(cache, conf);
    return fileSystem.getFileStatus(new Path(cache.getPath()));
  }

  /**
   * 获取HDFS上指定缓存文件的修改时间戳，供MapReduce内部使用。
   * @param conf 配置对象
   * @param cache 缓存文件的URI
   * @return 缓存文件的修改时间戳
   * @throws IOException 获取时间戳失败时抛出
   */
  @Deprecated
  public static long getTimestamp(Configuration conf, URI cache)
    throws IOException {
    return getFileStatus(conf, cache).getModificationTime();
  }

  /**
   * 设置归档文件的修改时间戳到配置中，用于本地化校验，供MapReduce内部使用。
   * @param conf 存储时间戳的配置对象
   * @param timestamps 逗号分隔的归档文件时间戳列表，顺序需与添加归档文件顺序一致
   */
  @Deprecated
  public static void setArchiveTimestamps(Configuration conf, String timestamps) {
    conf.set(CACHE_ARCHIVES_TIMESTAMPS, timestamps);
  }

  /**
   * 设置缓存文件的修改时间戳到配置中，用于本地化校验，供MapReduce内部使用。
   * @param conf 存储时间戳的配置对象
   * @param timestamps 逗号分隔的缓存文件时间戳列表，顺序需与添加文件顺序一致
   */
  @Deprecated
  public static void setFileTimestamps(Configuration conf, String timestamps) {
    conf.set(CACHE_FILES_TIMESTAMPS, timestamps);
  }

  /**
   * 设置本地化归档文件路径到配置中，供分布式缓存内部使用。
   * @param conf 要修改的配置对象
   * @param str 逗号分隔的本地归档文件路径列表
   */
  @Deprecated
  public static void setLocalArchives(Configuration conf, String str) {
    conf.set(CACHE_LOCALARCHIVES, str);
  }

  /**
   * 设置本地化文件路径到配置中，供分布式缓存内部使用。
   * @param conf 要修改的配置对象
   * @param str 逗号分隔的本地文件路径列表
   */
  @Deprecated
  public static void setLocalFiles(Configuration conf, String str) {
    conf.set(CACHE_LOCALFILES, str);
  }
}