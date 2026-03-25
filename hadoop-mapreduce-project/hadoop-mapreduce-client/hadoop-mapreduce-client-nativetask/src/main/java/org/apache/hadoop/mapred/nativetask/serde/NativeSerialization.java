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

package org.apache.hadoop.mapred.nativetask.serde;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Writable;

/**
 * 原生任务序列化工厂类，管理自定义Writable类型的原生序列化器注册与获取
 * 为MapReduce原生任务提供Java对象到原生内存格式的序列化支持，采用单例模式全局维护
 */
@InterfaceAudience.Private
public class NativeSerialization {

  // 存储类型名到对应序列化器类的映射缓存
  private final ConcurrentHashMap<String, Class<?>> map =
    new ConcurrentHashMap<String, Class<?>>();

  /**
   * 判断当前序列化工厂是否支持指定类型的序列化
   * @param c 待序列化类型
   * @return true如果该类型是Writable的子类，否则false
   */
  public boolean accept(Class<?> c) {
    return Writable.class.isAssignableFrom(c);
  }

  /**
   * 根据类型获取对应的原生序列化器实例
   * 优先使用用户注册的自定义序列化器，未注册则返回默认序列化器
   * @param c 待序列化类型
   * @return 序列化器实例
   * @throws IOException 类型不支持或实例化失败时抛出异常
   */
  @SuppressWarnings("unchecked")
  public INativeSerializer<Writable> getSerializer(Class<?> c) throws IOException {

    if (null == c) {
      return null;
    }
    // 仅支持Writable子类序列化
    if (!Writable.class.isAssignableFrom(c)) {
      throw new IOException("Cannot serialize type " + c.getName() +
                            ", we only accept subclass of Writable");
    }
    final String name = c.getName();
    final Class<?> serializer = map.get(name);

    if (null != serializer) {
      try {
        // 反射创建序列化器实例
        return (INativeSerializer<Writable>) serializer.newInstance();
      } catch (final Exception e) {
        throw new IOException(e);
      }
    }
    // 无自定义注册，返回默认序列化器
    return new DefaultSerializer();
  }

  /**
   * 注册指定类型的自定义原生序列化器
   * @param klass 目标Writable类型全限定名
   * @param serializer 自定义序列化器类
   * @throws IOException 参数无效、类型不匹配或重复注册不同序列化器时抛出异常
   */
  public void register(String klass, Class<?> serializer) throws IOException {
    if (null == klass || null == serializer) {
      throw new IOException("invalid arguments, klass or serializer is null");
    }

    // 验证序列化器实现了INativeSerializer接口
    if (!INativeSerializer.class.isAssignableFrom(serializer)) {
      throw new IOException("Serializer is not assigable from INativeSerializer");
    }

    final Class<?> storedSerializer = map.get(klass);
    if (null == storedSerializer) {
      // 保存类型与序列化器映射
      map.put(klass, serializer);
      return;
    } else {
      // 不允许同一类型注册不同的序列化器
      if (!storedSerializer.getName().equals(serializer.getName())) {
        throw new IOException("Error! Serializer already registered, existing: " +
                              storedSerializer.getName() + ", new: " +
                              serializer.getName());
      }
    }
  }

  /**
   * 清空所有已注册的序列化器映射
   */
  public void reset() {
    map.clear();
  }

  // 单例实例
  private static NativeSerialization instance = new NativeSerialization();

  /**
   * 获取NativeSerialization单例实例
   * @return 全局单例
   */
  public static NativeSerialization getInstance() {
    return instance;
  }
}