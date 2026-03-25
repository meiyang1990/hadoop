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

package org.apache.hadoop.mapreduce.filecache;

import java.io.*;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.net.URI;

/**
 * 文件级注释：MapReduce分布式缓存工具类，用于高效分发应用所需的大型只读文件到集群计算节点
 * 
 * Distribute application-specific large, read-only files efficiently.
 *
 * <p><code>DistributedCache</code> is a facility provided by the Map-Reduce
 * framework to cache files (text, archives, jars etc.) needed by applications.
 * </p>
 *
 * <p>Applications specify the files, via urls (hdfs:// or http://) to be cached
 * via the {@link org.apache.hadoop.mapred.JobConf}. The
 * <code>DistributedCache</code> assumes that the files specified via urls are
 * already present on the {@link FileSystem} at the path specified by the url
 * and are accessible by every machine in the cluster.</p>
 *
 * <p>The framework will copy the necessary files on to the worker node before
 * any tasks for the job are executed on that node. Its efficiency stems from
 * the fact that the files are only copied once per job and the ability to
 * cache archives which are un-archived on the workers.</p>
 *
 * <p><code>DistributedCache</code> can be used to distribute simple, read-only
 * data/text files and/or more complex types such as archives, jars etc.
 * Archives (zip, tar and tgz/tar.gz files) are un-archived at the worker nodes.
 * Jars may be optionally added to the classpath of the tasks, a rudimentary
 * software distribution mechanism.  Files have execution permissions.
 * In older version of Hadoop Map/Reduce users could optionally ask for symlinks
 * to be created in the working directory of the child task.  In the current
 * version symlinks are always created.  If the URL does not have a fragment
 * the name of the file or directory will be used. If multiple files or
 * directories map to the same link name, the last one added, will be used.  All
 * others will not even be downloaded.</p>
 *
 * <p><code>DistributedCache</code> tracks modification timestamps of the cache
 * files. Clearly the cache files should not be modified by the application
 * or externally while the job is executing.</p>
 *
 * <p>Here is an illustrative example on how to use the
 * <code>DistributedCache</code>:</p>
 * <p><blockquote><pre>
 *     // Setting up the cache for the application
 *
 *     1. Copy the requisite files to the <code>FileSystem</code>:
 *
 *     $ bin/hadoop fs -copyFromLocal lookup.dat /myapp/lookup.dat
 *     $ bin/hadoop fs -copyFromLocal map.zip /myapp/map.zip
 *     $ bin/hadoop fs -copyFromLocal mylib.jar /myapp/mylib.jar
 *     $ bin/hadoop fs -copyFromLocal mytar.tar /myapp/mytar.tar
 *     $ bin/hadoop fs -copyFromLocal mytgz.tgz /myapp/mytgz.tgz
 *     $ bin/hadoop fs -copyFromLocal mytargz.tar.gz /myapp/mytargz.tar.gz
 *
 *     2. Setup the application's <code>JobConf</code>:
 *
 *     JobConf job = new JobConf();
 *     DistributedCache.addCacheFile(new URI("/myapp/lookup.dat#lookup.dat"),
 *                                   job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/map.zip"), job);
 *     DistributedCache.addFileToClassPath(new Path("/myapp/mylib.jar"), job);
 *     DistributedCache.addCacheArchive(new URI("/mytar.tar"), job);
 *     DistributedCache.addCacheArchive(new URI("/mytgz.tgz"), job);
 *     DistributedCache.addCacheArchive(new URI("/mytargz.tar.gz"), job);
 *
 *     3. Use the cached files in the {@link org.apache.hadoop.mapred.Mapper}
 *     or {@link org.apache.hadoop.mapred.Reducer}:
 *
 *     public static class MapClass extends MapReduceBase
 *     implements Mapper&lt;K, V, K, V&gt; {
 *
 *       private Path[] localArchives;
 *       private Path[] localFiles;
 *
 *       public void configure(JobConf job) {
 *         // Get the cached archives/files
 *         File f = new File("./map.zip/some/file/in/zip.txt");
 *       }
 *
 *       public void map(K key, V value,
 *                       OutputCollector&lt;K, V&gt; output, Reporter reporter)
 *       throws IOException {
 *         // Use data from the cached archives/files here
 *         // ...
 *         // ...
 *         output.collect(k, v);
 *       }
 *     }
 *
 * </pre></blockquote>
 *
 * It is also very common to use the DistributedCache by using
 * {@link org.apache.hadoop.util.GenericOptionsParser}.
 *
 * This class includes methods that should be used by users
 * (specifically those mentioned in the example above, as well
 * as {@link DistributedCache#addArchiveToClassPath(Path, Configuration)}),
 * as well as methods intended for use by the MapReduce framework
 * (e.g., {@link org.apache.hadoop.mapred.JobClient}).
 *
 * @see org.apache.hadoop.mapreduce.Job
 * @see org.apache.hadoop.mapred.JobConf
 * @see org.apache.hadoop.mapred.JobClient
 */
@Deprecated
@InterfaceAudience.Private
/**
 * 类级注释：已废弃的分布式缓存工具类，所有方法已迁移到Job和JobContext相关API，保留此类仅为向后兼容
 * 核心职责：提供分布式缓存配置能力，将作业依赖的文件、归档包、Jar包分发到所有计算节点，实现节点级缓存复用
 */
public class DistributedCache {
  /** 通配符常量，用于匹配目录下所有文件 */
  public static final String WILDCARD = "*";
  
  /**
   * 将给定的归档包列表设置到作业配置中，供用户代码使用
   * @param archives 需要本地化的归档包URI列表
   * @param conf 要修改的作业配置对象
   * @deprecated Use {@link Job#setCacheArchives(URI[])} instead
   * @see Job#setCacheArchives(URI[])
   */
  @Deprecated
  public static void setCacheArchives(URI[] archives, Configuration conf) {
    Job.setCacheArchives(archives, conf);
  }

  /**
   * 将给定的缓存文件列表设置到作业配置中，供用户代码使用
   * @param files 需要本地化的文件URI列表
   * @param conf 要修改的作业配置对象
   * @deprecated Use {@link Job#setCacheFiles(URI[])} instead
   * @see Job#setCacheFiles(URI[])
   */
  @Deprecated
  public static void setCacheFiles(URI[] files, Configuration conf) {
    Job.setCacheFiles(files, conf);
  }

  /**
   * 从配置中获取已设置的缓存归档包列表，仅供内部框架代码使用
   * @param conf 包含缓存归档配置的作业配置对象
   * @return 配置中缓存归档的URI数组
   * @throws IOException
   * @deprecated Use {@link JobContext#getCacheArchives()} instead
   * @see JobContext#getCacheArchives()
   */
  @Deprecated
  public static URI[] getCacheArchives(Configuration conf) throws IOException {
    return JobContextImpl.getCacheArchives(conf);
  }

  /**
   * 从配置中获取已设置的缓存文件列表，仅供内部框架代码使用
   * @param conf 包含缓存文件配置的作业配置对象
   * @return 配置中缓存文件的URI数组
   * @throws IOException
   * @deprecated Use {@link JobContext#getCacheFiles()} instead
   * @see JobContext#getCacheFiles()
   */
  @Deprecated
  public static URI[] getCacheFiles(Configuration conf) throws IOException {
    return JobContextImpl.getCacheFiles(conf);
  }

  /**
   * 获取已本地化到节点的缓存归档路径数组，供用户代码使用
   * @param conf 包含本地化缓存路径的作业配置对象
   * @return 节点上缓存归档的本地路径数组
   * @throws IOException
   * @deprecated Use {@link JobContext#getLocalCacheArchives()} instead
   * @see JobContext#getLocalCacheArchives()
   */
  @Deprecated
  public static Path[] getLocalCacheArchives(Configuration conf) throws IOException {
    return JobContextImpl.getLocalCacheArchives(conf);
  }

  /**
   * 获取已本地化到节点的缓存文件路径数组，供用户代码使用
   * @param conf 包含本地化缓存路径的作业配置对象
   * @return 节点上缓存文件的本地路径数组
   * @throws IOException
   * @deprecated Use {@link JobContext#getLocalCacheFiles()} instead
   * @see JobContext#getLocalCacheFiles()
   */
  @Deprecated
  public static Path[] getLocalCacheFiles(Configuration conf)
    throws IOException {
    return JobContextImpl.getLocalCacheFiles(conf);
  }

  /**
   * 获取所有缓存归档的时间戳，仅供内部框架代码使用
   * @param conf 存储了时间戳的作业配置对象
   * @return 缓存归档时间戳数组
   * @deprecated Use {@link JobContext#getArchiveTimestamps()} instead
   * @see JobContext#getArchiveTimestamps()
   */
  @Deprecated
  public static long[] getArchiveTimestamps(Configuration conf) {
    return JobContextImpl.getArchiveTimestamps(conf);
  }


  /**
   * 获取所有缓存文件的时间戳，仅供内部框架代码使用
   * @param conf 存储了时间戳的作业配置对象
   * @return 缓存文件时间戳数组
   * @deprecated Use {@link JobContext#getFileTimestamps()} instead
   * @see JobContext#getFileTimestamps()
   */
  @Deprecated
  public static long[] getFileTimestamps(Configuration conf) {
    return JobContextImpl.getFileTimestamps(conf);
  }

  /**
   * 添加一个需要本地化的归档包到作业配置，供用户代码使用
   * @param uri 需要本地化的缓存归档URI
   * @param conf 要添加缓存配置的作业配置对象
   * @deprecated Use {@link Job#addCacheArchive(URI)} instead
   * @see Job#addCacheArchive(URI)
   */
  @Deprecated
  public static void addCacheArchive(URI uri, Configuration conf) {
    Job.addCacheArchive(uri, conf);
  }

  /**
   * 添加一个需要本地化的文件到作业配置，供用户代码使用
   * 本地化后的文件会下载到执行节点，并在作业工作目录创建软链接
   * 如果URI路径以"*"结尾，会本地化整个父目录并为目录下所有文件创建软链接
   * 文件访问权限决定是否可以跨作业共享缓存：文件不可读或父目录不可执行则无法共享
   * 
   * @param uri 需要本地化的缓存文件URI
   * @param conf 要添加缓存配置的作业配置对象
   * @deprecated Use {@link Job#addCacheFile(URI)} instead
   * @see Job#addCacheFile(URI)
   */
  @Deprecated
  public static void addCacheFile(URI uri, Configuration conf) {
    Job.addCacheFile(uri, conf);
  }

  /**
   * 添加文件到作业类路径，同时会将文件添加到分布式缓存，供用户代码使用
   *
   * @param file 需要添加的文件路径
   * @param conf 存储类路径配置的作业配置对象
   * @throws IOException
   * @deprecated Use {@link Job#addFileToClassPath(Path)} instead
   * @see #addCacheFile(URI, Configuration)
   * @see Job#addFileToClassPath(Path)
   */
  @Deprecated
  public static void addFileToClassPath(Path file, Configuration conf) throws IOException {
    Job.addFileToClassPath(file, conf, file.getFileSystem(conf));
  }

  /**
   * 添加文件到作业类路径，同时会将文件添加到分布式缓存，供用户代码使用
   *
   * @param file 需要添加的文件路径
   * @param conf 存储类路径配置的作业配置对象
   * @param fs 文件所在的文件系统对象
   */
  public static void addFileToClassPath(Path file, Configuration conf,
      FileSystem fs) {
    Job.addFileToClassPath(file, conf, fs, true);
  }

  /**
   * 添加文件到作业类路径，如果addToCache为true则同时添加到分布式缓存，仅供内部框架代码使用
   *
   * @param file 需要添加的文件路径
   * @param conf 存储类路径配置的作业配置对象
   * @param fs 文件所在的文件系统对象
   * @param addToCache 是否同时将文件添加到缓存列表
   * @see #addCacheFile(URI, Configuration)
   */
  public static void addFileToClassPath(Path file, Configuration conf,
      FileSystem fs, boolean addToCache) {
    Job.addFileToClassPath(file, conf, fs, addToCache);
  }

  /**
   * 获取类路径中所有文件条目路径数组，仅供内部框架代码使用
   *
   * @param conf 存储类路径配置的作业配置对象
   * @deprecated Use {@link JobContext#getFileClassPaths()} instead
   * @see JobContext#getFileClassPaths()
   */
  @Deprecated
  public static Path[] getFileClassPaths(Configuration conf) {
    return JobContextImpl.getFileClassPaths(conf);
  }

  /**
   * 添加归档包到作业类路径，同时会将归档包添加到分布式缓存，供用户代码使用
   *
   * @param archive 需要添加的归档包路径
   * @param conf 存储类路径配置的作业配置对象
   * @throws IOException
   * @deprecated Use {@link Job#addArchiveToClassPath(Path)} instead
   * @see Job#addArchiveToClassPath(Path)
   */
  @Deprecated
  public static void addArchiveToClassPath(Path archive, Configuration conf)
    throws IOException {
    Job.addArchiveToClassPath(archive, conf, archive.getFileSystem(conf));
  }

  /**
   * 添加归档包到作业类路径，同时会将归档包添加到分布式缓存，供用户代码使用
   *
   * @param archive 需要添加的归档包路径
   * @param conf 存储类路径配置的作业配置对象
   * @param fs 归档包所在的文件系统对象
   * @throws IOException
   */
  public static void addArchiveToClassPath
         (Path archive, Configuration conf, FileSystem fs)
      throws IOException {
    Job.addArchiveToClassPath(archive, conf, fs);
  }

  /**
   * 获取类路径中所有归档包条目路径数组，仅供内部框架代码使用
   *
   * @param conf 存储类路径配置的作业配置对象
   * @deprecated Use {@link JobContext#getArchiveClassPaths()} instead 
   * @see JobContext#getArchiveClassPaths()
   */
  @Deprecated
  public static Path[] getArchiveClassPaths(Configuration conf) {
    return JobContextImpl.getArchiveClassPaths(conf);
  }

  /**
   * 原本用于启用软链接，当前版本软链接始终启用无法禁用，此方法为空操作
   * @param conf 作业配置对象
   * @deprecated This is a NO-OP.
   */
  @Deprecated
  public static void createSymlink(Configuration conf){
    //NOOP
  }

  /**
   * 原本用于检查是否需要创建软链接，当前版本软链接始终启用无法禁用
   * @param conf 作业配置对象
   * @return 始终返回true
   * @deprecated symlinks are always created.
   */
  @Deprecated
  public static boolean getSymlink(Configuration conf){
    return true;
  }

  /**
   * 将字符串数组解析为布尔数组，用于批量转换可见性配置
   * @param strs 待转换的字符串数组
   * @return 转换后的布尔数组
   */
  private static boolean[] parseBooleans(String[] strs) {
    if (null == strs) {
      return null;
    }
    boolean[] result = new boolean[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Boolean.parseBoolean(strs[i]);
    }
    return