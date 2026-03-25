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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;

/**
 * MapReduce原生任务平台抽象基类。
 * 平台指运行在MapReduce之上的计算框架（如Hive、Pig、Mahout等），
 * 每个框架定义了自己的键值类型规范，本类为不同框架提供统一扩展接口，
 * 用于注册序列化器、支持原生排序和原生侧数据交互。
 * Hadoop已经提供{@link HadoopPlatform}支持原生Hadoop键类型，用户可以自定义实现适配自己的框架。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Platform {
  private final NativeSerialization serialization;
  protected Set<String> keyClassNames = new HashSet<String>();

  /**
   * 构造函数，获取原生序列化管理器实例
   */
  public Platform() {
    this.serialization = NativeSerialization.getInstance();
  }

  /**
   * 初始化平台，注册当前平台支持的所有键类型和对应序列化器
   * @throws IOException 初始化失败时抛出IO异常
   */
  public abstract void init() throws IOException;

  /**
   * 获取平台名称，用于日志输出和调试
   * @return 平台名称字符串
   */
  public abstract String name();


  /**
   * 注册键类型，将键类与对应序列化器绑定到当前平台
   * @param keyClassName 输出键类全限定名
   * @param key 键序列化器类
   * @throws IOException 注册失败时抛出IO异常
   */
  protected void registerKey(String keyClassName, Class<?> key) throws IOException {
    serialization.register(keyClassName, key);
    keyClassNames.add(keyClassName);
  }

  /**
   * 判断当前平台是否支持指定键的原生处理
   * 需要满足两个条件：1、键属于当前平台；2、对应序列化器实现了{@link INativeComparable}接口支持原生排序
   * @param keyClassName 输出键类全限定名
   * @param serializer   通过registerKey注册的对应序列化器
   * @param job          作业配置对象
   * @return             当前平台实现了该键的原生比较器返回true，否则返回false
   */
  protected abstract boolean support(String keyClassName,
      INativeSerializer<?> serializer, JobConf job);


  /**
   * 判断自定义Java比较器是否是当前平台定义的
   * 原生任务默认不支持用户自定义Java比较器（通过mapreduce.job.output.key.comparator.class配置），
   * 但部分平台（如Pig）会设置该配置并同时实现原生比较器，这种情况下不应该中断任务执行
   * @param keyComparator 配置中指定的键比较器类
   * @return 是当前平台定义的返回true，否则返回false
   */
  protected abstract boolean define(Class<?> keyComparator);
}