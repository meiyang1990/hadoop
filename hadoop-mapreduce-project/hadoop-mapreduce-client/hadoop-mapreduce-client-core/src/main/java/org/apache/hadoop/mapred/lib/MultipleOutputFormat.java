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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Progressable;

/**
 * 文件输入格式抽象扩展类，支持将MapReduce输出数据写入多个不同输出文件。
 * 支持三种典型使用场景：
 * <ol>
 * <li>带Reducer的作业：Reducer根据Key/Value将数据输出到不同文件</li>
 * <li>仅Map作业：输出文件名基于输入文件名生成或衍生</li>
 * <li>仅Map作业：输出文件名同时依赖输入文件名和数据Key</li>
 * </ol>
 * 是旧MapReduce API的多输出实现，新API请参考org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MultipleOutputFormat<K, V>
extends FileOutputFormat<K, V> {

  /**
   * 创建复合记录写入器，支持将不同键值对写入不同输出文件
   * @param fs 文件系统对象
   * @param job 作业配置对象
   * @param name 基础输出文件名（如part-00000）
   * @param arg3 进度报告对象
   * @return 复合记录写入器实例
   * @throws IOException 创建写入器出错时抛出
   */
  public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf job,
      String name, Progressable arg3) throws IOException {

    final FileSystem myFS = fs;
    final String myName = generateLeafFileName(name);
    final JobConf myJob = job;
    final Progressable myProgressable = arg3;

    return new RecordWriter<K, V>() {

      // 缓存不同输出路径对应的记录写入器
      TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap<String, RecordWriter<K, V>>();

      public void write(K key, V value) throws IOException {

        // 根据当前Key生成输出文件路径
        String keyBasedPath = generateFileNameForKeyValue(key, value, myName);

        // 结合输入文件名生成最终输出路径
        String finalPath = getInputFileBasedOutputFileName(myJob, keyBasedPath);

        // 提取实际输出Key（原始Key中包含路径信息时使用）
        K actualKey = generateActualKey(key, value);
        // 提取实际输出Value（原始Value中包含路径信息时使用）
        V actualValue = generateActualValue(key, value);

        // 从缓存获取对应路径的写入器
        RecordWriter<K, V> rw = this.recordWriters.get(finalPath);
        if (rw == null) {
          // 缓存不存在则新建写入器并加入缓存
          rw = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
          this.recordWriters.put(finalPath, rw);
        }
        // 写入实际键值对
        rw.write(actualKey, actualValue);
      };

      public void close(Reporter reporter) throws IOException {
        // 遍历关闭所有缓存的写入器
        Iterator<String> keys = this.recordWriters.keySet().iterator();
        while (keys.hasNext()) {
          RecordWriter<K, V> rw = this.recordWriters.get(keys.next());
          rw.close(reporter);
        }
        // 清空缓存
        this.recordWriters.clear();
      };
    };
  }

  /**
   * 生成输出文件的基础文件名，默认不修改原始基础文件名
   * @param name 原始基础文件名（如part-00000）
   * @return 处理后的基础文件名
   */
  protected String generateLeafFileName(String name) {
    return name;
  }

  /**
   * 根据当前键值对生成输出文件名，默认不基于键值对修改文件名
   * @param key 当前输出数据的Key
   * @param value 当前输出数据的Value
   * @param name 基础文件名
   * @return 生成的输出文件名
   */
  protected String generateFileNameForKeyValue(K key, V value, String name) {
    return name;
  }

  /**
   * 从原始键值对中提取实际输出Key，默认直接返回原始Key
   * 当原始Key中同时包含输出路径和实际Key时，子类可覆盖此方法提取实际Key
   * @param key 原始输入Key
   * @param value 原始输入Value
   * @return 提取后的实际输出Key
   */
  protected K generateActualKey(K key, V value) {
    return key;
  }
  
  /**
   * 从原始键值对中提取实际输出Value，默认直接返回原始Value
   * 当原始Value中同时包含输出路径和实际Value时，子类可覆盖此方法提取实际Value
   * @param key 原始输入Key
   * @param value 原始输入Value
   * @return 提取后的实际输出Value
   */
  protected V generateActualValue(K key, V value) {
    return value;
  }
  

  /**
   * 结合输入文件路径生成最终输出文件名，支持提取输入路径的后N段组成输出文件名
   * 仅当作业为仅Map作业且配置了需要保留的路径段数时才会修改文件名，否则返回原文件名
   * @param job 作业配置对象
   * @param name 当前生成的输出文件名
   * @return 结合输入路径生成的最终输出文件名
   */
  protected String getInputFileBasedOutputFileName(JobConf job, String name) {
    String infilepath = job.get(MRJobConfig.MAP_INPUT_FILE);
    if (infilepath == null) {
      // 不存在输入文件信息（非仅Map作业），直接返回原文件名
      return name;
    }
    // 获取配置的需要保留的输入路径尾段数量
    int numOfTrailingLegsToUse = job.getInt("mapred.outputformat.numOfTrailingLegs", 0);
    if (numOfTrailingLegsToUse <= 0) {
      // 未配置或配置不合法，直接返回原文件名
      return name;
    }
    // 从输入路径提取后N段拼接为输出文件名
    Path infile = new Path(infilepath);
    Path parent = infile.getParent();
    String midName = infile.getName();
    Path outPath = new Path(midName);
    for (int i = 1; i < numOfTrailingLegsToUse; i++) {
      if (parent == null) break;
      midName = parent.getName();
      if (midName.length() == 0) break;
      parent = parent.getParent();
      outPath = new Path(midName, outPath);
    }
    return outPath.toString();
  }

  /**
   * 抽象方法，子类实现获取指定文件的基础记录写入器
   * @param fs 文件系统对象
   * @param job 作业配置对象
   * @param name 输出文件名
   * @param arg3 进度报告对象
   * @return 指定文件的记录写入器
   * @throws IOException 创建写入器出错时抛出
   */
  abstract protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs,
      JobConf job, String name, Progressable arg3) throws IOException;
}