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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件级注释：DelegatingInputFormat 是支持多输入路径不同InputFormat的委托输入格式实现
 * 核心功能：为同一个MapReduce作业中不同输入路径配置不同的InputFormat和Mapper，将分片逻辑委托给对应路径的实际InputFormat处理
 * 
 * 支持多输入路径绑定不同InputFormat和Mapper的代理输入格式，将操作代理给各路径对应的实际输入格式执行
 * 
 * @see MultipleInputs#addInputPath(Job, Path, Class, Class)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingInputFormat<K, V> extends InputFormat<K, V> {

  /**
   * 生成所有输入分片，按输入格式和Mapper分组，将分片逻辑委托给对应实际输入格式处理
   * @param job 作业上下文
   * @return 聚合所有分组后的输入分片列表
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext job) 
      throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    Job jobCopy = Job.getInstance(conf);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    // 从MultipleInputs获取路径->输入格式的映射关系
    Map<Path, InputFormat> formatMap = 
      MultipleInputs.getInputFormatMap(job);
    // 从MultipleInputs获取路径->Mapper类型的映射关系
    Map<Path, Class<? extends Mapper>> mapperMap = MultipleInputs
       .getMapperTypeMap(job);
    // 构建输入格式类型->路径列表的分组映射
    Map<Class<? extends InputFormat>, List<Path>> formatPaths
        = new HashMap<Class<? extends InputFormat>, List<Path>>();

    // 第一步：按输入格式类型对路径进行分组，同类型输入格式的路径归为一组
    for (Entry<Path, InputFormat> entry : formatMap.entrySet()) {
      if (!formatPaths.containsKey(entry.getValue().getClass())) {
       formatPaths.put(entry.getValue().getClass(), new LinkedList<Path>());
      }

      formatPaths.get(entry.getValue().getClass()).add(entry.getKey());
    }

    // 遍历每一组同输入格式类型的路径
    for (Entry<Class<? extends InputFormat>, List<Path>> formatEntry : 
        formatPaths.entrySet()) {
      Class<? extends InputFormat> formatClass = formatEntry.getKey();
      // 通过反射实例化实际输入格式对象
      InputFormat format = (InputFormat) ReflectionUtils.newInstance(
         formatClass, conf);
      List<Path> paths = formatEntry.getValue();

      // 在同一输入格式组内，再按Mapper类型对路径进行分组
      Map<Class<? extends Mapper>, List<Path>> mapperPaths
          = new HashMap<Class<? extends Mapper>, List<Path>>();

      // 第二步：在相同输入格式组内，按Mapper类型对路径分组
      for (Path path : paths) {
       Class<? extends Mapper> mapperClass = mapperMap.get(path);
       if (!mapperPaths.containsKey(mapperClass)) {
         mapperPaths.put(mapperClass, new LinkedList<Path>());
       }

       mapperPaths.get(mapperClass).add(path);
      }

      // 对相同输入格式+相同Mapper的路径组，统一生成分片
      for (Entry<Class<? extends Mapper>, List<Path>> mapEntry :
          mapperPaths.entrySet()) {
       paths = mapEntry.getValue();
       Class<? extends Mapper> mapperClass = mapEntry.getKey();

       // 如果路径未指定Mapper，使用作业全局默认Mapper
       if (mapperClass == null) {
         try {
           mapperClass = job.getMapperClass();
         } catch (ClassNotFoundException e) {
           throw new IOException("Mapper class is not found", e);
         }
       }

       // 为当前路径组设置输入路径到作业副本
       FileInputFormat.setInputPaths(jobCopy, paths.toArray(new Path[paths
           .size()]));

       // 调用实际输入格式生成分片，包装为TaggedInputSplit绑定输入格式和Mapper信息
       List<InputSplit> pathSplits = format.getSplits(jobCopy);
       for (InputSplit pathSplit : pathSplits) {
         splits.add(new TaggedInputSplit(pathSplit, conf, format.getClass(),
             mapperClass));
       }
      }
    }

    return splits;
  }

  /**
   * 创建委托记录读取器，将读取操作代理给分片绑定的实际记录读取器
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @return 委托记录读取器实例
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new DelegatingRecordReader<K, V>(split, context);
  }
}