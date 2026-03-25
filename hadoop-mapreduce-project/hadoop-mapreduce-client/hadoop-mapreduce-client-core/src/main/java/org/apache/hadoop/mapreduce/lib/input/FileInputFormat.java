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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FileUtil.maybeIgnoreMissingDirectory;

/**
 * 文件型InputFormat的抽象基类，为所有基于文件的输入格式提供通用实现。
 * 核心功能包括：输入路径管理、文件过滤、输入分片切分逻辑，子类只需实现具体的记录读取逻辑。
 * 子类可以重写{@link #isSplitable(JobContext, Path)}方法控制文件是否可切分，默认所有文件均可切分。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormat<K, V> extends InputFormat<K, V> {
  /** 输入目录配置项key */
  public static final String INPUT_DIR = 
    "mapreduce.input.fileinputformat.inputdir";
  /** 分片最大字节数配置项key */
  public static final String SPLIT_MAXSIZE = 
    "mapreduce.input.fileinputformat.split.maxsize";
  /** 分片最小字节数配置项key */
  public static final String SPLIT_MINSIZE = 
    "mapreduce.input.fileinputformat.split.minsize";
  /** 路径过滤器类配置项key */
  public static final String PATHFILTER_CLASS = 
    "mapreduce.input.pathFilter.class";
  /** 输入文件总数配置项key */
  public static final String NUM_INPUT_FILES =
    "mapreduce.input.fileinputformat.numinputfiles";
  /** 是否递归遍历输入目录配置项key */
  public static final String INPUT_DIR_RECURSIVE =
    "mapreduce.input.fileinputformat.input.dir.recursive";
  /** 非递归模式下是否忽略子目录配置项key */
  public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS =
    "mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs";
  /** 列取文件状态的线程数配置项key */
  public static final String LIST_STATUS_NUM_THREADS =
      "mapreduce.input.fileinputformat.list-status.num-threads";
  /** 列取文件状态的默认线程数 */
  public static final int DEFAULT_LIST_STATUS_NUM_THREADS = 1;

  private static final Logger LOG =
      LoggerFactory.getLogger(FileInputFormat.class);

  /** 分片大小容差阈值：剩余文件大小超过splitSize的10%才会生成新分片 */
  private static final double SPLIT_SLOP = 1.1;   // 10% slop
  
  @Deprecated
  public enum Counter {
    BYTES_READ
  }

  /** 默认隐藏文件过滤器，过滤掉以_或.开头的文件 */
  private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
      }
    }; 

  /**
   * 多过滤器组合路径过滤器，仅当所有内部过滤器都accept时才接受路径。
   * 用于组合默认隐藏文件过滤器和用户自定义过滤器。
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
   * 设置作业是否递归遍历输入目录
   * @param job 待修改的作业对象
   * @param inputDirRecursive 是否递归遍历
   */
  public static void setInputDirRecursive(Job job,
      boolean inputDirRecursive) {
    job.getConfiguration().setBoolean(INPUT_DIR_RECURSIVE,
        inputDirRecursive);
  }
 
  /**
   * 获取作业是否开启输入目录递归遍历
   * @param job 作业上下文对象
   * @return 是否递归遍历输入目录
   */
  public static boolean getInputDirRecursive(JobContext job) {
    return job.getConfiguration().getBoolean(INPUT_DIR_RECURSIVE,
        false);
  }

  /**
   * 获取当前输入格式要求的最小分片大小
   * @return 格式要求的最小分片字节数
   */
  protected long getFormatMinSplitSize() {
    return 1;
  }

  /**
   * 判断指定文件是否可以切分为多个分片
   * 默认实现返回true，处理不可切分文件（如压缩文件）的子类必须重写此方法。
   * 返回false可保证整个文件作为一个分片由单个Mapper处理。
   * @param context 作业上下文
   * @param filename 待检查的文件路径
   * @return 文件是否可切分
   */
  protected boolean isSplitable(JobContext context, Path filename) {
    return true;
  }

  /**
   * 设置作业输入路径的过滤器
   * @param job 待修改的作业对象
   * @param filter 路径过滤器类
   */
  public static void setInputPathFilter(Job job,
                                        Class<? extends PathFilter> filter) {
    job.getConfiguration().setClass(PATHFILTER_CLASS, filter, 
                                    PathFilter.class);
  }

  /**
   * 设置输入分片最小字节数
   * @param job 待修改的作业对象
   * @param size 最小分片字节数
   */
  public static void setMinInputSplitSize(Job job,
                                          long size) {
    job.getConfiguration().setLong(SPLIT_MINSIZE, size);
  }

  /**
   * 获取输入分片最小字节数配置
   * @param job 作业上下文
   * @return 最小分片字节数
   */
  public static long getMinSplitSize(JobContext job) {
    return job.getConfiguration().getLong(SPLIT_MINSIZE, 1L);
  }

  /**
   * 设置输入分片最大字节数
   * @param job 待修改的作业对象
   * @param size 最大分片字节数
   */
  public static void setMaxInputSplitSize(Job job,
                                          long size) {
    job.getConfiguration().setLong(SPLIT_MAXSIZE, size);
  }

  /**
   * 获取输入分片最大字节数配置
   * @param context 作业上下文
   * @return 最大分片字节数
   */
  public static long getMaxSplitSize(JobContext context) {
    return context.getConfiguration().getLong(SPLIT_MAXSIZE, 
                                              Long.MAX_VALUE);
  }

  /**
   * 获取作业配置的输入路径过滤器实例
   * @param context 作业上下文
   * @return 路径过滤器实例，未配置则返回null
   */
  public static PathFilter getInputPathFilter(JobContext context) {
    Configuration conf = context.getConfiguration();
    Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null,
        PathFilter.class);
    return (filterClass != null) ?
        (PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /**
   * 列取所有输入路径匹配的文件状态，应用过滤器并获取权限token
   * 如果开启安全认证，会收集输入路径的委托令牌并添加到作业凭证中
   * @param job 作业上下文
   * @return 所有符合条件的输入文件状态列表
   * @throws IOException 当没有输入路径或IO错误时抛出异常
   */
  protected List<FileStatus> listStatus(JobContext job
                                        ) throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    
    // 为所有输入文件系统获取NameNode委托令牌
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, 
                                        job.getConfiguration());

    // 获取是否递归遍历目录配置
    boolean recursive = getInputDirRecursive(job);

    // 组合默认隐藏文件过滤器和用户自定义过滤器
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);
    
    List<FileStatus> result = null;

    // 获取列取文件状态的线程数配置
    int numThreads = job.getConfiguration().getInt(LIST_STATUS_NUM_THREADS,
        DEFAULT_LIST_STATUS_NUM_THREADS);
    StopWatch sw = new StopWatch().start();
    if (numThreads == 1) {
      // 单线程列取
      result = singleThreadedListStatus(job, dirs, inputFilter, recursive);
    } else {
      // 多线程并发列取
      Iterable<FileStatus> locatedFiles = null;
      try {
        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
            job.getConfiguration(), dirs, recursive, inputFilter, true);
        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
      } catch (InterruptedException e) {
        throw (IOException)
            new InterruptedIOException(
                "Interrupted while getting file statuses")
                .initCause(e);
      }
      result = Lists.newArrayList(locatedFiles);
    }
    
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to get FileStatuses: "
          + sw.now(TimeUnit.MILLISECONDS));
    }
    LOG.info("Total input files to process : " + result.size());
    return result;
  }

  /**
   * 单线程列取输入路径下所有符合过滤条件的文件状态
   * @param job 作业上下文
   * @param dirs 输入路径数组
   * @param inputFilter 路径过滤器
   * @param recursive 是否递归遍历子目录
   * @return 符合条件的文件状态列表
   * @throws IOException 当输入路径错误或IO异常时抛出
   */
  private List<FileStatus> singleThreadedListStatus(JobContext job, Path[] dirs,
      PathFilter inputFilter, boolean recursive) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();
    for (int i=0; i < dirs.length; ++i) {
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job.getConfiguration()); 
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            // 遍历目录下的文件
            RemoteIterator<LocatedFileStatus> iter =
                fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  // 递归添加子目录中的文件
                  addInputPathRecursively(result, fs, stat.getPath(),
                      inputFilter);
                } else {
                  result.add(shrinkStatus(stat));
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
   * 递归遍历目录将所有符合过滤条件的文件添加到结果列表
   * @param result 存储结果文件状态的列表
   * @param fs 文件系统对象
   * @param path 当前遍历的目录路径
   * @param inputFilter 路径过滤器
   * @throws IOException IO异常
   */
  protected void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    // 捕获列出目录过程中可能出现的文件找不到异常
    try {
      RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
      while (iter.hasNext()) {
        LocatedFileStatus stat = iter.next();
        if (inputFilter.accept(stat.getPath())) {
          if (stat.isDirectory()) {
            // 递归遍历子目录
            addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
          } else {
            result.add(shrinkStatus(stat));
          }
        }
      }
    } catch (FileNotFoundException e) {
      // 根据文件系统能力决定是否忽略消失的目录
      maybeIgnoreMissingDirectory(fs, path, e);
    }
  }

  /**
   * 压缩LocatedFileStatus占用的内存空间，移除HdfsBlockLocation中冗余的LocatedBlock引用
   * 在作业提交阶段这些冗余信息没有用处，压缩后可以减少内存占用，支持扫描更多输入文件
   * @param origStat 原始文件状态对象
   * @return 压缩后的文件状态对象
   */
  public static FileStatus shrinkStatus(FileStatus origStat) {
    if (origStat.isDirectory() || origStat.getLen() == 0 ||
        !(origStat instanceof LocatedFileStatus)) {
      return origStat;
    } else {
      // 重构块位置信息，移除冗余引用
      BlockLocation[] blockLocations =
          ((LocatedFileStatus)origStat).getBlockLocations();
      BlockLocation[] locs = new BlockLocation[blockLocations.length];
      int i = 0;
      for (BlockLocation location : blockLocations) {
        locs[i++] = new BlockLocation(location);
      }
      LocatedFileStatus newStat = new LocatedFileStatus(origStat, locs);
      return newStat;
    }
  }

  /**
   * 创建FileSplit工厂方法，子类可重写创建自定义分片类型
   * @param file 分片所属文件路径
   * @param start 分片在文件中的起始偏移
   * @param length 分片字节长度
   * @param hosts 分片数据所在的节点主机列表
   * @return 创建好的FileSplit对象
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }
  
  /**
   * 创建带内存块位置的FileSplit工厂方法，子类可重写创建自定义分片类型
   * @param file 分片所属文件路径
   * @param start 分片在文件中的起始偏移
   * @param length 分片字节长度
   * @param hosts 分片数据所在的节点主机列表
   * @param inMemoryHosts 缓存了分片数据的节点主机列表
   * @return 创建好的FileSplit对象
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts, String[] inMemoryHosts) {
    return new FileSplit(file, start, length, hosts, inMemoryHosts);
  }

  /**
   * 根据输入文件生成分片列表，按照配置的分片大小规则对文件进行切分
   * 考虑数据本地性，将分片和数据所在节点绑定，方便调度器分配任务
   * @param job 作业上下文
   * @return 生成的输入分片列表
   * @throws IOException IO异常
   */
  public List<InputSplit> getSplits(JobContext job)