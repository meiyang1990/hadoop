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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件级注释：支持MapReduce作业配置多输入路径，每个路径可以指定独立的InputFormat和Mapper实现
 * 用于处理不同输入路径格式不同、需要不同Mapper处理的场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleInputs {
  // 配置项：存储路径到InputFormat的映射关系
  public static final String DIR_FORMATS = 
    "mapreduce.input.multipleinputs.dir.formats";
  // 配置项：存储路径到Mapper的映射关系
  public static final String DIR_MAPPERS = 
    "mapreduce.input.multipleinputs.dir.mappers";
  
  /**
   * 向当前作业添加一个自定义InputFormat的输入路径
   * @param job 目标MapReduce作业对象
   * @param path 需要添加的输入路径
   * @param inputFormatClass 该路径使用的InputFormat类型
   */
  @SuppressWarnings("unchecked")
  public static void addInputPath(Job job, Path path,
      Class<? extends InputFormat> inputFormatClass) {
    // 构造路径与InputFormat的映射字符串：路径;类全限定名
    String inputFormatMapping = path.toString() + ";"
       + inputFormatClass.getName();
    Configuration conf = job.getConfiguration();
    String inputFormats = conf.get(DIR_FORMATS);
    // 将新映射追加到配置中，多个映射用逗号分隔
    conf.set(DIR_FORMATS,
       inputFormats == null ? inputFormatMapping : inputFormats + ","
           + inputFormatMapping);

    // 设置作业使用代理InputFormat处理多输入
    job.setInputFormatClass(DelegatingInputFormat.class);
  }

  /**
   * 向当前作业添加一个自定义InputFormat和自定义Mapper的输入路径
   * @param job 目标MapReduce作业对象
   * @param path 需要添加的输入路径
   * @param inputFormatClass 该路径使用的InputFormat类型
   * @param mapperClass 该路径使用的Mapper类型
   */
  @SuppressWarnings("unchecked")
  public static void addInputPath(Job job, Path path,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass) {

    // 先添加路径和InputFormat映射
    addInputPath(job, path, inputFormatClass);
    Configuration conf = job.getConfiguration();
    // 构造路径与Mapper的映射字符串：路径;类全限定名
    String mapperMapping = path.toString() + ";" + mapperClass.getName();
    String mappers = conf.get(DIR_MAPPERS);
    // 将新映射追加到配置中，多个映射用逗号分隔
    conf.set(DIR_MAPPERS, mappers == null ? mapperMapping
       : mappers + "," + mapperMapping);

    // 设置作业使用代理Mapper处理多输入不同Mapper
    job.setMapperClass(DelegatingMapper.class);
  }

  /**
   * 从作业配置中解析路径到InputFormat实例的映射表
   * @param job 作业上下文对象
   * @return 路径对应InputFormat实例的映射表
   */
  @SuppressWarnings("unchecked")
  static Map<Path, InputFormat> getInputFormatMap(JobContext job) {
    Map<Path, InputFormat> m = new HashMap<Path, InputFormat>();
    Configuration conf = job.getConfiguration();
    // 拆分逗号分隔的多个映射
    String[] pathMappings = conf.get(DIR_FORMATS).split(",");
    for (String pathMapping : pathMappings) {
      // 拆分路径和类名
      String[] split = pathMapping.split(";");
      InputFormat inputFormat;
      try {
        // 通过反射创建InputFormat实例
       inputFormat = (InputFormat) ReflectionUtils.newInstance(conf
           .getClassByName(split[1]), conf);
      } catch (ClassNotFoundException e) {
       throw new RuntimeException(e);
      }
      // 将路径和实例放入映射表
      m.put(new Path(split[0]), inputFormat);
    }
    return m;
  }

  /**
   * 从作业配置中解析路径到Mapper类型的映射表
   * @param job 作业上下文对象
   * @return 路径对应Mapper类型的映射表
   */
  @SuppressWarnings("unchecked")
  static Map<Path, Class<? extends Mapper>> 
      getMapperTypeMap(JobContext job) {
    Configuration conf = job.getConfiguration();
    // 没有配置自定义Mapper时返回空映射
    if (conf.get(DIR_MAPPERS) == null) {
      return Collections.emptyMap();
    }
    Map<Path, Class<? extends Mapper>> m = 
      new HashMap<Path, Class<? extends Mapper>>();
    // 拆分逗号分隔的多个映射
    String[] pathMappings = conf.get(DIR_MAPPERS).split(",");
    for (String pathMapping : pathMappings) {
      // 拆分路径和类名
      String[] split = pathMapping.split(";");
      Class<? extends Mapper> mapClass;
      try {
        // 加载Mapper类对象
       mapClass = 
         (Class<? extends Mapper>) conf.getClassByName(split[1]);
      } catch (ClassNotFoundException e) {
       throw new RuntimeException(e);
      }
      // 将路径和类放入映射表
      m.put(new Path(split[0]), mapClass);
    }
    return m;
  }
}