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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件：hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/lib/MultipleInputs.java
 * 支持MapReduce作业配置多个输入路径，每个路径可指定不同的InputFormat和Mapper实现
 * 用于处理不同格式输入数据源、不同业务逻辑需要不同Mapper处理的多输入场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleInputs {
  /**
   * 向MapReduce作业添加带自定义InputFormat的输入路径
   * @param conf 作业配置对象
   * @param path 要添加的输入路径
   * @param inputFormatClass 该路径使用的InputFormat类
   */
  public static void addInputPath(JobConf conf, Path path,
      Class<? extends InputFormat> inputFormatClass) {

    // 构造路径与InputFormat的映射字符串
    String inputFormatMapping = path.toString() + ";"
       + inputFormatClass.getName();
    // 从配置中获取已有的映射信息
    String inputFormats = conf.get("mapreduce.input.multipleinputs.dir.formats");
    // 将新映射追加写入配置
    conf.set("mapreduce.input.multipleinputs.dir.formats",
       inputFormats == null ? inputFormatMapping : inputFormats + ","
           + inputFormatMapping);

    // 设置作业使用委派InputFormat处理多输入分片
    conf.setInputFormat(DelegatingInputFormat.class);
  }

  /**
   * 向MapReduce作业添加带自定义InputFormat和自定义Mapper的输入路径
   * @param conf 作业配置对象
   * @param path 要添加的输入路径
   * @param inputFormatClass 该路径使用的InputFormat类
   * @param mapperClass 该路径使用的Mapper类
   */
  public static void addInputPath(JobConf conf, Path path,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass) {

    // 先添加InputFormat映射配置
    addInputPath(conf, path, inputFormatClass);

    // 构造路径与Mapper的映射字符串
    String mapperMapping = path.toString() + ";" + mapperClass.getName();
    // 从配置中获取已有的映射信息
    String mappers = conf.get("mapreduce.input.multipleinputs.dir.mappers");
    // 将新映射追加写入配置
    conf.set("mapreduce.input.multipleinputs.dir.mappers", mappers == null ? mapperMapping
       : mappers + "," + mapperMapping);

    // 设置作业使用委派Mapper处理多输入映射
    conf.setMapperClass(DelegatingMapper.class);
  }

  /**
   * 从作业配置中解析路径到对应InputFormat实例的映射表
   * @param conf 作业配置对象
   * @return 路径到InputFormat实例的映射表
   */
  static Map<Path, InputFormat> getInputFormatMap(JobConf conf) {
    Map<Path, InputFormat> m = new HashMap<Path, InputFormat>();
    // 切分所有路径与InputFormat的映射字符串
    String[] pathMappings = conf.get("mapreduce.input.multipleinputs.dir.formats").split(",");
    // 遍历解析每个映射
    for (String pathMapping : pathMappings) {
      String[] split = pathMapping.split(";");
      InputFormat inputFormat;
      try {
        // 通过反射实例化InputFormat对象
        inputFormat = (InputFormat) ReflectionUtils.newInstance(conf
            .getClassByName(split[1]), conf);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      // 将路径与实例存入映射表
      m.put(new Path(split[0]), inputFormat);
    }
    return m;
  }

  /**
   * 从作业配置中解析路径到对应Mapper类的映射表
   * @param conf 作业配置对象
   * @return 路径到Mapper类的映射表，无配置时返回空映射
   */
  @SuppressWarnings("unchecked")
  static Map<Path, Class<? extends Mapper>> getMapperTypeMap(JobConf conf) {
    // 无配置时返回空映射
    if (conf.get("mapreduce.input.multipleinputs.dir.mappers") == null) {
      return Collections.emptyMap();
    }
    Map<Path, Class<? extends Mapper>> m = new HashMap<Path, Class<? extends Mapper>>();
    // 切分所有路径与Mapper的映射字符串
    String[] pathMappings = conf.get("mapreduce.input.multipleinputs.dir.mappers").split(",");
    // 遍历解析每个映射
    for (String pathMapping : pathMappings) {
      String[] split = pathMapping.split(";");
      Class<? extends Mapper> mapClass;
      try {
        // 加载Mapper类对象
        mapClass = (Class<? extends Mapper>) conf.getClassByName(split[1]);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      // 将路径与类存入映射表
      m.put(new Path(split[0]), mapClass);
    }
    return m;
  }
}