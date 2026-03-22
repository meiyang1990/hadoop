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

package org.apache.hadoop.mapreduce.lib.aggregate;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * 文件：用户自定义值聚合描述符包装类
 * 属于Hadoop MapReduce Aggregate聚合框架，用于动态加载和封装用户自定义的聚合描述符
 * 核心职责：1. 动态加载用户自定义的ValueAggregatorDescriptor实现类并实例化
 *          2. 将generateKeyValPairs等方法调用委托给实际用户自定义实现对象执行
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UserDefinedValueAggregatorDescriptor implements
    ValueAggregatorDescriptor {
  private String className;

  protected ValueAggregatorDescriptor theAggregatorDescriptor = null;

  private static final Class<?>[] argArray = new Class[] {};

  /**
   * 根据类名动态加载并实例化指定类
   * @param className 需要实例化的类全限定名
   * @return 动态创建的类实例对象
   */
  public static Object createInstance(String className) {
    Object retv = null;
    try {
      // 获取当前线程上下文类加载器，用于加载用户自定义类
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      // 加载目标类到JVM
      Class<?> theFilterClass = Class.forName(className, true, classLoader);
      // 获取无参构造方法
      Constructor<?> meth = theFilterClass.getDeclaredConstructor(argArray);
      // 绕过访问权限检查，允许实例化私有类
      meth.setAccessible(true);
      // 通过反射创建实例
      retv = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return retv;
  }

  /**
   * 创建用户自定义聚合描述符实例并完成配置
   * @param conf Hadoop配置对象
   */
  private void createAggregator(Configuration conf) {
    if (theAggregatorDescriptor == null) {
      // 反射实例化用户自定义类
      theAggregatorDescriptor = (ValueAggregatorDescriptor)
                                  createInstance(this.className);
      // 对实例进行配置初始化
      theAggregatorDescriptor.configure(conf);
    }
  }

  /**
   * 构造方法，初始化包装类并实例化用户自定义聚合描述符
   * @param className 用户自定义聚合描述符实现类的全限定名
   * @param conf 用于配置聚合描述符的配置对象
   */
  public UserDefinedValueAggregatorDescriptor(String className, 
      Configuration conf) {
    this.className = className;
    this.createAggregator(conf);
  }

  /**
   * 将调用委托给实际用户自定义聚合描述符，生成聚合键值对列表
   * @param key 输入数据的键
   * @param val 输入数据的值
   * @return 聚合ID/值对列表，聚合ID编码了聚合类型，用于指导Reduce/Combiner阶段的聚合逻辑
   */
  public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key,
                                                          Object val) {
    ArrayList<Entry<Text, Text>> retv = new ArrayList<Entry<Text, Text>>();
    if (this.theAggregatorDescriptor != null) {
      retv = this.theAggregatorDescriptor.generateKeyValPairs(key, val);
    }
    return retv;
  }

  /**
   * 获取当前对象的字符串表示，包含用户自定义类名
   * @return 对象字符串描述
   */
  public String toString() {
    return "UserDefinedValueAggregatorDescriptor with class name:" + "\t"
      + this.className;
  }

  /**
   * 配置方法，此处无需额外配置，空实现
   * @param conf Hadoop配置对象
   */
  public void configure(Configuration conf) {

  }

}