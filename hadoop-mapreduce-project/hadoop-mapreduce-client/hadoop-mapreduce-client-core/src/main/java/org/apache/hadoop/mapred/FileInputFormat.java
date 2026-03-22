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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件型InputFormat的抽象基类，为所有基于文件的输入格式提供通用能力
 * 
 * <p>提供了输入路径解析、文件过滤、输入分片生成的通用实现，子类只需要实现
 * {@link #getRecordReader(InputSplit, JobConf, Reporter)} 方法即可。
 * 子类可以重写 {@link #isSplitable(FileSystem, Path)} 方法控制文件是否可分割，
 * 默认实现认为所有文件都可以分割。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormat<K, V> implements InputFormat<K, V> {

  public static final Logger LOG =
      LoggerFactory.getLogger(FileInputFormat.class);
  
  @Deprecated
  public enum Counter {
    BYTES_READ
  }

  public static final String NUM_INPUT_FILES =
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.NUM_INPUT_FILES;

  public static final String INPUT_DIR_RECURSIVE = 
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE;

  public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS =
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS;


  private static final double SPLIT_SLOP = 1.1;   // 10% slop，分片大小容差，剩余大小超过该比例才生成新分片

  private long minSplitSize = 1;
  // 过滤隐藏文件：过滤文件名以_或.开头的文件
  private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
      }
    }; 
  protected void setMinSplitSize(long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  /**
   * 多过滤器组合PathFilter，只有所有过滤器都接受的路径才会被接受
   * 用于同时应用内置的隐藏文件过滤器和用户自定义过滤器
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * 判断给定文件是否可分割，默认实现总是返回true
   * 需要处理不可分割文件（如流式压缩文件）的子类必须重写此方法
   * 返回false可以保证整个文件作为一个分片，由单个Mapper处理
   * 
   * @param fs 文件所在文件系统
   * @param filename 待检查的文件路径
   * @return 文件是否可分割
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return true;
  }
  
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

  /**
   * 设置MapReduce作业输入路径的过滤器类，用于过滤不需要处理的输入路径
   *
   * @param filter 用于过滤输入路径的PathFilter类
   */
  public static void setInputPathFilter(JobConf conf,
                                        Class<? extends PathFilter> filter) {
    conf.setClass(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.PATHFILTER_CLASS, filter, PathFilter.class);
  }

  /**
   * 获取作业配置中设置的输入路径过滤器实例
   *
   * @return 配置好的输入路径过滤器实例，如果没有设置则返回null
   */
  public static PathFilter getInputPathFilter(JobConf conf) {
    Class<? extends PathFilter> filterClass = conf.getClass(
	  org.apache.hadoop.mapreduce.lib.input.FileInputFormat.PATHFILTER_CLASS,
	  null, PathFilter.class);
    return (filterClass != null) ?
        ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /**
   * 递归遍历输入路径，将所有符合过滤条件的文件添加到结果列表
   * @param result 存储所有符合条件文件的列表
   * @param fs 文件系统对象
   * @param path 当前遍历的输入路径
   * @param inputFilter 输入路径过滤器
   * @throws IOException IO异常
   */
  protected void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      if (inputFilter.accept(stat.getPath())) {
        if (stat.isDirectory()) {
          addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
        } else {
          result.add(org.apache.hadoop.mapreduce.lib.input.
              FileInputFormat.shrinkStatus(stat));
        }
      }
    }
  }
  
  /**
   * 枚举所有输入路径下的输入文件，处理通配符匹配和安全凭证获取
   * 开启安全认证时，会自动获取所有输入文件系统的委托令牌添加到作业凭证中
   * @param job 当前作业配置
   * @return 所有符合条件的输入文件状态数组
   * @throws IOException 当没有输入路径或匹配不到文件时抛出IO异常
   */
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // 为所有输入文件系统获取访问令牌
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);
    
    // 是否递归遍历子目录
    boolean recursive = job.getBoolean(INPUT_DIR_RECURSIVE, false);

    // 组合内置隐藏文件过滤器和用户自定义过滤器
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    FileStatus[] result;
    // 获取文件状态枚举的并发线程数配置
    int numThreads = job
        .getInt(
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS,
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.DEFAULT_LIST_STATUS_NUM_THREADS);
    
    StopWatch sw = new StopWatch().start();
    if (numThreads == 1) {
      // 单线程枚举输入文件
      List<FileStatus> locatedFiles = singleThreadedListStatus(job, dirs, inputFilter, recursive); 
      result = locatedFiles.toArray(new FileStatus[locatedFiles.size()]);
    } else {
      // 多线程并发枚举输入文件
      Iterable<FileStatus> locatedFiles = null;
      try {
        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
            job, dirs, recursive, inputFilter, false);
        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
      } catch (InterruptedException e) {
        throw  (IOException)
            new InterruptedIOException("Interrupted while getting file statuses")
                .initCause(e);
      }
      result = Iterables.toArray(locatedFiles, FileStatus.class);
    }

    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to get FileStatuses: "
          + sw.now(TimeUnit.MILLISECONDS));
    }
    LOG.info("Total input files to process : " + result.length);
    return result;
  }
  
  /**
   * 单线程枚举输入路径下所有符合条件的文件
   */
  private List<FileStatus> singleThreadedListStatus(JobConf job, Path[] dirs,
      PathFilter inputFilter, boolean recursive) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();
    for (Path p: dirs) {
      FileSystem fs = p.getFileSystem(job); 
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter =
                fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(),
                      inputFilter);
                } else {
                  result.add(org.apache.hadoop.mapreduce.lib.input.
                      FileInputFormat.shrinkStatus(stat));
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }
    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    return result;
  }

  /**
   * 生成FileSplit分片的工厂方法，子类可以重写该方法生成自定义分片类型
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }
  
  /**
   * 生成带缓存主机信息的FileSplit分片工厂方法，子类可以重写该方法生成自定义分片类型
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts, String[] inMemoryHosts) {
    return new FileSplit(file, start, length, hosts, inMemoryHosts);
  }

  /**
   * 将输入文件切分成分片，每个分片由一个Map任务处理
   * 会根据文件块位置信息，尽量保证数据本地性，减少网络传输
   * @param job 当前作业配置
   * @param numSplits 期望生成的分片数量
   * @return 切分完成的输入分片数组
   * @throws IOException IO异常
   */
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    // 获取所有输入文件状态
    FileStatus[] stats = listStatus(job);

    // 保存输入文件数量到作业配置，用于指标统计
    job.setLong(NUM_INPUT_FILES, stats.length);
    long totalSize = 0;
    // 非递归模式下是否忽略子目录
    boolean ignoreDirs = !job.getBoolean(INPUT_DIR_RECURSIVE, false)
      && job.getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);

    List<FileStatus> files = new ArrayList<>(stats.length);
    // 过滤掉目录，只保留有效文件
    for (FileStatus file: stats) {
      if (file.isDirectory()) {
        if (!ignoreDirs) {
          throw new IOException("Not a file: "+ file.getPath());
        }
      } else {
        files.add(file);
        totalSize += file.getLen();
      }
    }

    // 计算每个分片目标大小：总大小除以期望分片数
    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    // 计算分片最小大小，取作业配置和当前类设置的最大值
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // 生成分片列表
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          // 从LocatedFileStatus直接获取块位置信息
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          // 从文件系统查询块位置信息
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        // 判断文件是否可分割
        if (isSplitable(fs, path)) {
          long blockSize = file.getBlockSize();
          // 计算最终分片大小：在minSize和goalSize之间，尽量不超过blockSize
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          // 剩余大小超过容差比例，继续生成分片
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            // 获取分片存储位置的主机和缓存主机列表
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          // 处理剩余字节
          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
          // 文件不可分割，整个文件作为一个分片
          if (LOG.isDebugEnabled()) {
            // 只有文件足够大时才打印日志
            if (length > Math.min(file.getBlockSize(), minSize)) {
              LOG.debug("File is not splittable so no parallelization "
                  + "is possible: " + file.getPath());
            }
          }
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
      } else { 
        // 空文件创建空分片
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  /**
   * 计算最终分片大小，取minSize、goalSize、blockSize三者的中间值
   * 保证分片大小不小于最小分片，不大于块大小，尽量接近目标分片大小
   */
  protected long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));