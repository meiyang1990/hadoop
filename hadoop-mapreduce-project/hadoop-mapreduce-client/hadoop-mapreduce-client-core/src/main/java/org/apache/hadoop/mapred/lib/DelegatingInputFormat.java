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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 多路输入代理InputFormat，将不同路径的输入处理委托给对应的具体InputFormat实现
 * 支持同一个MapReduce作业中使用不同格式的输入路径，并为每个路径指定不同的Mapper
 * 
 * @see MultipleInputs#addInputPath(JobConf, Path, Class, Class)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingInputFormat<K, V> implements InputFormat<K, V> {

  /**
   * 聚合所有输入路径的分片，按InputFormat和Mapper分组后分别分片，最终返回统一的分片列表
   * @param conf 作业配置对象
   * @param numSplits 期望分片数量
   * @return 聚合后的输入分片数组
   * @throws IOException IO异常
   */
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {

    JobConf confCopy = new JobConf(conf);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    // 获取路径到对应InputFormat的映射
    Map<Path, InputFormat> formatMap = MultipleInputs.getInputFormatMap(conf);
    // 获取路径到对应Mapper类型的映射
    Map<Path, Class<? extends Mapper>> mapperMap = MultipleInputs
       .getMapperTypeMap(conf);
    // 按InputFormat类型分组，存储该类型对应的所有输入路径
    Map<Class<? extends InputFormat>, List<Path>> formatPaths
        = new HashMap<Class<? extends InputFormat>, List<Path>>();

    // 第一步：按InputFormat类型对路径分组
    for (Entry<Path, InputFormat> entry : formatMap.entrySet()) {
      if (!formatPaths.containsKey(entry.getValue().getClass())) {
       formatPaths.put(entry.getValue().getClass(), new LinkedList<Path>());
      }

      formatPaths.get(entry.getValue().getClass()).add(entry.getKey());
    }

    // 遍历每组相同InputFormat的路径
    for (Entry<Class<? extends InputFormat>, List<Path>> formatEntry : 
        formatPaths.entrySet()) {
      Class<? extends InputFormat> formatClass = formatEntry.getKey();
      // 反射创建InputFormat实例
      InputFormat format = (InputFormat) ReflectionUtils.newInstance(
         formatClass, conf);
      List<Path> paths = formatEntry.getValue();

      // 同InputFormat下再按Mapper类型分组
      Map<Class<? extends Mapper>, List<Path>> mapperPaths
          = new HashMap<Class<? extends Mapper>, List<Path>>();

      // 第二步：同InputFormat下按Mapper类型对路径分组
      for (Path path : paths) {
       Class<? extends Mapper> mapperClass = mapperMap.get(path);
       if (!mapperPaths.containsKey(mapperClass)) {
         mapperPaths.put(mapperClass, new LinkedList<Path>());
       }

       mapperPaths.get(mapperClass).add(path);
      }

      // 第三步：对每组相同InputFormat和Mapper的路径一起进行分片
      for (Entry<Class<? extends Mapper>, List<Path>> mapEntry : mapperPaths
         .entrySet()) {
       paths = mapEntry.getValue();
       Class<? extends Mapper> mapperClass = mapEntry.getKey();

       // 如果路径未指定Mapper，使用作业全局默认Mapper
       if (mapperClass == null) {
         mapperClass = conf.getMapperClass();
       }

       // 设置当前分组的输入路径到配置中
       FileInputFormat.setInputPaths(confCopy, paths.toArray(new Path[paths
           .size()]));

       // 调用对应InputFormat生成分片，并包装为带格式和Mapper信息的TaggedInputSplit
       InputSplit[] pathSplits = format.getSplits(confCopy, numSplits);
       for (InputSplit pathSplit : pathSplits) {
         splits.add(new TaggedInputSplit(pathSplit, conf, format.getClass(),
             mapperClass));
       }
      }
    }

    return splits.toArray(new InputSplit[splits.size()]);
  }

  /**
   * 根据分片携带的信息，委托对应InputFormat创建RecordReader读取分片数据
   * @param split 输入分片，实际为TaggedInputSplit携带原始分片和格式信息
   * @param conf 作业配置对象
   * @param reporter 进度报告器
   * @return 对应输入分片的RecordReader实例
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked")
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf conf,
      Reporter reporter) throws IOException {

    // 从代理分片中解压出原始信息
    TaggedInputSplit taggedInputSplit = (TaggedInputSplit) split;
    // 反射创建对应InputFormat实例
    InputFormat<K, V> inputFormat = (InputFormat<K, V>) ReflectionUtils
       .newInstance(taggedInputSplit.getInputFormatClass(), conf);
    // 委托目标InputFormat获取RecordReader读取原始分片
    return inputFormat.getRecordReader(taggedInputSplit.getInputSplit(), conf,
       reporter);
  }
}