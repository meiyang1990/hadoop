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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;

/**
 * 文件：带标签的输入分片，为原生InputSplit附加输入格式和Mapper类信息
 * 供DelegatingInputFormat和DelegatingMapper使用，支持多输入格式多Mapper的作业场景
 * 实现了InputSplit接口和Configurable接口，可被Hadoop框架序列化和反序列化
 */
class TaggedInputSplit implements Configurable, InputSplit {

  private Class<? extends InputSplit> inputSplitClass;

  private InputSplit inputSplit;

  private Class<? extends InputFormat> inputFormatClass;

  private Class<? extends Mapper> mapperClass;

  private Configuration conf;

  public TaggedInputSplit() {
    // Default constructor.
  }

  /**
   * 构造一个带标签的输入分片
   * 
   * @param inputSplit 待标记的原始输入分片
   * @param conf 作业配置对象
   * @param inputFormatClass 该分片对应的输入格式类
   * @param mapperClass 该分片对应的Mapper处理类
   */
  public TaggedInputSplit(InputSplit inputSplit, Configuration conf,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass) {
    this.inputSplitClass = inputSplit.getClass();
    this.inputSplit = inputSplit;
    this.conf = conf;
    this.inputFormatClass = inputFormatClass;
    this.mapperClass = mapperClass;
  }

  /**
   * 获取被标记的原始输入分片
   * 
   * @return 原始输入分片对象
   */
  public InputSplit getInputSplit() {
    return inputSplit;
  }

  /**
   * 获取该分片对应的输入格式类
   * 
   * @return 输入格式类对象
   */
  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  /**
   * 获取该分片对应的Mapper处理类
   * 
   * @return Mapper处理类对象
   */
  public Class<? extends Mapper> getMapperClass() {
    return mapperClass;
  }

  @Override
  public long getLength() throws IOException {
    return inputSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return inputSplit.getLocations();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取分片类类型
    inputSplitClass = (Class<? extends InputSplit>) readClass(in);
    // 通过反射实例化分片对象
    inputSplit = (InputSplit) ReflectionUtils
       .newInstance(inputSplitClass, conf);
    // 读取分片自身字段
    inputSplit.readFields(in);
    // 读取输入格式类
    inputFormatClass = (Class<? extends InputFormat>) readClass(in);
    // 读取Mapper处理类
    mapperClass = (Class<? extends Mapper>) readClass(in);
  }

  /**
   * 从序列化输入中读取类名并加载类对象
   * @param in 数据输入流
   * @return 加载后的类对象
   * @throws IOException IO异常
   */
  private Class<?> readClass(DataInput in) throws IOException {
    // 读取类名字符串，使用弱引用缓存
    String className = StringInterner.weakIntern(Text.readString(in));
    try {
      // 从配置中加载类
      return conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("readObject can't find class", e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // 写入原始分片类名
    Text.writeString(out, inputSplitClass.getName());
    // 写入原始分片序列化数据
    inputSplit.write(out);
    // 写入输入格式类名
    Text.writeString(out, inputFormatClass.getName());
    // 写入Mapper类名
    Text.writeString(out, mapperClass.getName());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return inputSplit.toString();
  }

}