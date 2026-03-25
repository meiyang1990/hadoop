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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;

/**
 * 为DelegatingInputFormat和DelegatingMapper设计的包装类，给原始InputSplit添加额外标签信息
 * 用于在多输入格式作业中，为每个分片绑定对应的输入格式和Mapper类
 */
class TaggedInputSplit extends InputSplit implements Configurable, Writable {

  private Class<? extends InputSplit> inputSplitClass;

  private InputSplit inputSplit;

  @SuppressWarnings("unchecked")
  private Class<? extends InputFormat> inputFormatClass;

  @SuppressWarnings("unchecked")
  private Class<? extends Mapper> mapperClass;

  private Configuration conf;

  /**
   * 默认构造方法，用于反序列化
   */
  public TaggedInputSplit() {
    // Default constructor.
  }

  /**
   * 创建带标签的InputSplit包装对象
   * 
   * @param inputSplit 待包装的原始InputSplit
   * @param conf 作业配置对象
   * @param inputFormatClass 该分片对应的InputFormat类
   * @param mapperClass 该分片对应的Mapper类
   */
  @SuppressWarnings("unchecked")
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
   * 获取被包装的原始InputSplit
   * 
   * @return 原始未包装的InputSplit
   */
  public InputSplit getInputSplit() {
    return inputSplit;
  }

  /**
   * 获取当前分片对应的InputFormat类
   * 
   * @return InputFormat类对象
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  /**
   * 获取当前分片对应的Mapper类
   * 
   * @return Mapper类对象
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Mapper> getMapperClass() {
    return mapperClass;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    // 委托给原始InputSplit计算分片大小
    return inputSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    // 委托给原始InputSplit获取分片所在节点位置
    return inputSplit.getLocations();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取原始InputSplit类信息
    inputSplitClass = (Class<? extends InputSplit>) readClass(in);
    // 读取InputFormat类信息
    inputFormatClass = (Class<? extends InputFormat<?, ?>>) readClass(in);
    // 读取Mapper类信息
    mapperClass = (Class<? extends Mapper<?, ?, ?, ?>>) readClass(in);
    // 通过反射实例化原始InputSplit对象
    inputSplit = (InputSplit) ReflectionUtils
       .newInstance(inputSplitClass, conf);
    // 创建序列化工厂，获取对应反序列化器
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer deserializer = factory.getDeserializer(inputSplitClass);
    // 打开输入流反序列化原始InputSplit
    deserializer.open((DataInputStream)in);
    inputSplit = (InputSplit)deserializer.deserialize(inputSplit);
  }

  /**
   * 从输入流中读取类名并加载类对象
   * @param in 输入流
   * @return 加载完成的类对象
   * @throws IOException 读取IO异常或类找不到异常
   */
  private Class<?> readClass(DataInput in) throws IOException {
    // 读取类名字符串，使用弱引用驻留字符串减少内存占用
    String className = StringInterner.weakIntern(Text.readString(in));
    try {
      // 从配置中加载对应类
      return conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("readObject can't find class", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(DataOutput out) throws IOException {
    // 写入原始InputSplit类名
    Text.writeString(out, inputSplitClass.getName());
    // 写入InputFormat类名
    Text.writeString(out, inputFormatClass.getName());
    // 写入Mapper类名
    Text.writeString(out, mapperClass.getName());
    // 创建序列化工厂，获取对应序列化器
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer serializer = 
          factory.getSerializer(inputSplitClass);
    // 打开输出流序列化原始InputSplit
    serializer.open((DataOutputStream)out);
    serializer.serialize(inputSplit);
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
    // 直接返回原始InputSplit的字符串表示
    return inputSplit.toString();
  }

}