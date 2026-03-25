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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop原生任务平台实现，负责注册和管理Hadoop标准Writable类型的原生序列化器
 * 为原生MapReduce任务提供Hadoop内置数据类型的序列化支持
 */
@InterfaceAudience.Private
public class HadoopPlatform extends Platform {
  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopPlatform.class);

  /**
   * 构造Hadoop平台实例
   * @throws IOException 初始化失败时抛出异常
   */
  public HadoopPlatform() throws IOException {
  }

  /**
   * 初始化Hadoop平台，注册所有Hadoop内置Writable类型对应的原生序列化器
   * @throws IOException 初始化过程中IO异常
   */
  @Override
  public void init() throws IOException {
    // 注册NullWritable类型序列化器
    registerKey(NullWritable.class.getName(), NullWritableSerializer.class);
    // 注册Text类型序列化器
    registerKey(Text.class.getName(), TextSerializer.class);
    // 注册LongWritable类型序列化器
    registerKey(LongWritable.class.getName(), LongWritableSerializer.class);
    // 注册IntWritable类型序列化器
    registerKey(IntWritable.class.getName(), IntWritableSerializer.class);
    // 注册默认Writable类型序列化器，处理未单独注册的自定义Writable
    registerKey(Writable.class.getName(), DefaultSerializer.class);
    // 注册BytesWritable类型序列化器
    registerKey(BytesWritable.class.getName(), BytesWritableSerializer.class);
    // 注册BooleanWritable类型序列化器
    registerKey(BooleanWritable.class.getName(), BoolWritableSerializer.class);
    // 注册ByteWritable类型序列化器
    registerKey(ByteWritable.class.getName(), ByteWritableSerializer.class);
    // 注册FloatWritable类型序列化器
    registerKey(FloatWritable.class.getName(), FloatWritableSerializer.class);
    // 注册DoubleWritable类型序列化器
    registerKey(DoubleWritable.class.getName(), DoubleWritableSerializer.class);
    // 注册可变长度IntWritable类型序列化器
    registerKey(VIntWritable.class.getName(), VIntWritableSerializer.class);
    // 注册可变长度LongWritable类型序列化器
    registerKey(VLongWritable.class.getName(), VLongWritableSerializer.class);

    LOG.info("Hadoop platform inited");
  }

  /**
   * 检查当前平台是否支持指定键类型和序列化器
   * @param keyClassName 键类全限定名
   * @param serializer 键对应的序列化器
   * @param job 作业配置对象
   * @return true表示支持，false表示不支持
   */
  @Override
  public boolean support(String keyClassName, INativeSerializer<?> serializer, JobConf job) {
    // 检查键类已注册且序列化器实现了原生比较接口
    if (keyClassNames.contains(keyClassName)
      && serializer instanceof INativeComparable) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * 定义自定义比较器，本平台不支持自定义比较器
   * @param comparatorClass 比较器类
   * @return 始终返回false，表示不支持
   */
  @Override
  public boolean define(Class<?> comparatorClass) {
    return false;
  }

  /**
   * 获取平台名称
   * @return 平台名称"Hadoop"
   */
  @Override
  public String name() {
    return "Hadoop";
  }
}