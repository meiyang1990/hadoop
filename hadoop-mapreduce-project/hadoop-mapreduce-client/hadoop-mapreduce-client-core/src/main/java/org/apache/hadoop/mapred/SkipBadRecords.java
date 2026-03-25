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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * MapReduce坏记录跳过功能工具类，定义了坏记录跳过相关的所有配置参数与访问方法。
 * 
 * <p>Hadoop提供可选执行模式，当任务多次失败时，会自动检测并跳过导致崩溃的坏记录，
 * 保证任务能够完成，仅损失少量数据。适用于用户业务逻辑或第三方库存在bug，无法修复
 * 且允许少量数据丢失的场景。</p>
 * 
 * <p>工作原理：开启跳过模式后，任务处理前会向TaskTracker上报当前即将处理的记录范围，
 * 如果任务崩溃，TaskTracker会记录该范围，后续重试时直接跳过该范围，保证任务能完成。</p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SkipBadRecords {
  
  /**
   * 坏记录检测计数器组名称，用于框架检测坏记录，应用需要递增该组下对应计数器。
   */
  public static final String COUNTER_GROUP = "SkippingTaskCounters";
  
  /**
   * Map端已处理记录计数器名称。
   * @see SkipBadRecords#getAutoIncrMapperProcCount(Configuration)
   */
  public static final String COUNTER_MAP_PROCESSED_RECORDS = 
    "MapProcessedRecords";
  
  /**
   * Reduce端已处理分组计数器名称。
   * @see SkipBadRecords#getAutoIncrReducerProcCount(Configuration)
   */
  public static final String COUNTER_REDUCE_PROCESSED_GROUPS = 
    "ReduceProcessedGroups";
  
  private static final String ATTEMPTS_TO_START_SKIPPING = 
    JobContext.SKIP_START_ATTEMPTS;
  private static final String AUTO_INCR_MAP_PROC_COUNT = 
    JobContext.MAP_SKIP_INCR_PROC_COUNT;
  private static final String AUTO_INCR_REDUCE_PROC_COUNT = 
    JobContext.REDUCE_SKIP_INCR_PROC_COUNT;
  private static final String OUT_PATH = JobContext.SKIP_OUTDIR;
  private static final String MAPPER_MAX_SKIP_RECORDS = 
    JobContext.MAP_SKIP_MAX_RECORDS;
  private static final String REDUCER_MAX_SKIP_GROUPS = 
    JobContext.REDUCE_SKIP_MAXGROUPS;
  
  /**
   * 获取触发跳过模式所需的任务失败重试次数，失败次数超过该值后会开启跳过模式。
   * 默认值为2。
   * 
   * @param conf 配置对象
   * @return 触发跳过模式的任务尝试次数
   */
  public static int getAttemptsToStartSkipping(Configuration conf) {
    return conf.getInt(ATTEMPTS_TO_START_SKIPPING, 2);
  }

  /**
   * 设置触发跳过模式所需的任务失败重试次数，失败次数超过该值后会开启跳过模式。
   * 默认值为2。
   * 
   * @param conf 配置对象
   * @param attemptsToStartSkipping 触发跳过模式的任务尝试次数
   */
  public static void setAttemptsToStartSkipping(Configuration conf, 
      int attemptsToStartSkipping) {
    conf.setInt(ATTEMPTS_TO_START_SKIPPING, attemptsToStartSkipping);
  }

  /**
   * 获取Map端处理记录计数器是否自动递增标记。
   * 如果为true，MapRunner会在调用map函数后自动递增COUNTER_MAP_PROCESSED_RECORDS。
   * 如果应用异步处理或缓冲输入记录（如Hadoop Streaming），需要设为false，由应用自行递增计数器。
   * 默认值为true。
   * 
   * @param conf 配置对象
   * @return true表示自动递增，false表示应用自行递增
   */
  public static boolean getAutoIncrMapperProcCount(Configuration conf) {
    return conf.getBoolean(AUTO_INCR_MAP_PROC_COUNT, true);
  }
  
  /**
   * 设置Map端处理记录计数器是否自动递增标记。
   * 如果为true，MapRunner会在调用map函数后自动递增COUNTER_MAP_PROCESSED_RECORDS。
   * 如果应用异步处理或缓冲输入记录（如Hadoop Streaming），需要设为false，由应用自行递增计数器。
   * 默认值为true。
   * 
   * @param conf 配置对象
   * @param autoIncr true表示自动递增，false表示应用自行递增
   */
  public static void setAutoIncrMapperProcCount(Configuration conf, 
      boolean autoIncr) {
    conf.setBoolean(AUTO_INCR_MAP_PROC_COUNT, autoIncr);
  }
  
  /**
   * 获取Reduce端处理分组计数器是否自动递增标记。
   * 如果为true，框架会在调用reduce函数后自动递增COUNTER_REDUCE_PROCESSED_GROUPS。
   * 如果应用异步处理或缓冲输入分组（如Hadoop Streaming），需要设为false，由应用自行递增计数器。
   * 默认值为true。
   * 
   * @param conf 配置对象
   * @return true表示自动递增，false表示应用自行递增
   */
  public static boolean getAutoIncrReducerProcCount(Configuration conf) {
    return conf.getBoolean(AUTO_INCR_REDUCE_PROC_COUNT, true);
  }
  
  /**
   * 设置Reduce端处理分组计数器是否自动递增标记。
   * 如果为true，框架会在调用reduce函数后自动递增COUNTER_REDUCE_PROCESSED_GROUPS。
   * 如果应用异步处理或缓冲输入分组（如Hadoop Streaming），需要设为false，由应用自行递增计数器。
   * 默认值为true。
   * 
   * @param conf 配置对象
   * @param autoIncr true表示自动递增，false表示应用自行递增
   */
  public static void setAutoIncrReducerProcCount(Configuration conf, 
      boolean autoIncr) {
    conf.setBoolean(AUTO_INCR_REDUCE_PROC_COUNT, autoIncr);
  }
  
  /**
   * 获取被跳过记录的输出目录路径，跳过的坏记录会写入该目录。
   * 默认输出到作业输出目录下_logs/skip子目录，设置为null可关闭跳过记录写入。
   * 
   * @param conf 配置对象
   * @return 跳过记录输出路径，未配置且无作业输出路径时返回null
   */
  public static Path getSkipOutputPath(Configuration conf) {
    String name =  conf.get(OUT_PATH);
    if(name!=null) {
      if("none".equals(name)) {
        return null;
      }
      return new Path(name);
    }
    Path outPath = FileOutputFormat.getOutputPath(new JobConf(conf));
    return outPath==null ? null : new Path(outPath, 
        "_logs"+Path.SEPARATOR+"skip");
  }
  
  /**
   * 设置被跳过记录的输出目录路径，跳过的坏记录会写入该目录。
   * 默认输出到作业输出目录下_logs/skip子目录，设置为null可关闭跳过记录写入。
   * 
   * @param conf 配置对象
   * @param path 跳过记录输出路径
   */
  public static void setSkipOutputPath(JobConf conf, Path path) {
    String pathStr = null;
    if(path==null) {
      pathStr = "none";
    } else {
      pathStr = path.toString();
    }
    conf.set(OUT_PATH, pathStr);
  }
  
  /**
   * 获取Mapper端单个坏记录允许跳过的最大记录数（包含坏记录本身）。
   * 设置为0可关闭坏记录检测跳过功能；设置为Long.MAX_VALUE表示无需缩小跳过范围，接受任意范围跳过。
   * 框架会通过多次重试逐步缩小跳过范围，直到满足该阈值或用尽重试次数。
   * 默认值为0。
   * 
   * @param conf 配置对象
   * @return 允许跳过的最大记录数
   */
  public static long getMapperMaxSkipRecords(Configuration conf) {
    return conf.getLong(MAPPER_MAX_SKIP_RECORDS, 0);
  }
  
  /**
   * 设置Mapper端单个坏记录允许跳过的最大记录数（包含坏记录本身）。
   * 设置为0可关闭坏记录检测跳过功能；设置为Long.MAX_VALUE表示无需缩小跳过范围，接受任意范围跳过。
   * 框架会通过多次重试逐步缩小跳过范围，直到满足该阈值或用尽重试次数。
   * 默认值为0。
   * 
   * @param conf 配置对象
   * @param maxSkipRecs 允许跳过的最大记录数
   */
  public static void setMapperMaxSkipRecords(Configuration conf, 
      long maxSkipRecs) {
    conf.setLong(MAPPER_MAX_SKIP_RECORDS, maxSkipRecs);
  }
  
  /**
   * 获取Reducer端单个坏分组允许跳过的最大分组数（包含坏分组本身）。
   * 设置为0可关闭坏分组检测跳过功能；设置为Long.MAX_VALUE表示无需缩小跳过范围，接受任意范围跳过。
   * 框架会通过多次重试逐步缩小跳过范围，直到满足该阈值或用尽重试次数。
   * 默认值为0。
   * 
   * @param conf 配置对象
   * @return 允许跳过的最大分组数
   */
  public static long getReducerMaxSkipGroups(Configuration conf) {
    return conf.getLong(REDUCER_MAX_SKIP_GROUPS, 0);
  }
  
  /**
   * 设置Reducer端单个坏分组允许跳过的最大分组数（包含坏分组本身）。
   * 设置为0可关闭坏分组检测跳过功能；设置为Long.MAX_VALUE表示无需缩小跳过范围，接受任意范围跳过。
   * 框架会通过多次重试逐步缩小跳过范围，直到满足该阈值或用尽重试次数。
   * 默认值为0。
   * 
   * @param conf 配置对象
   * @param maxSkipGrps 允许跳过的最大分组数
   */
  public static void setReducerMaxSkipGroups(Configuration conf, 
      long maxSkipGrps) {
    conf.setLong(REDUCER_MAX_SKIP_GROUPS, maxSkipGrps);
  }
}