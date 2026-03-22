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
import java.util.ServiceLoader;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 原生任务平台管理类，负责加载类路径下所有平台实现并提供统一访问入口
 * 作为外观类，提供键类型支持检查、比较器定义等平台相关操作的统一入口
 */
@InterfaceAudience.Private
public class Platforms {

  private static final Logger LOG = LoggerFactory.getLogger(Platforms.class);
  // 通过SPI机制加载所有Platform实现
  private static final ServiceLoader<Platform> platforms = ServiceLoader.load(Platform.class);
  
  /**
   * 初始化所有加载到的平台，注册原生序列化实现
   * @param conf Hadoop配置对象
   * @throws IOException 初始化失败时抛出IO异常
   */
  public static void init(Configuration conf) throws IOException {

    // 重置原生序列化实例，清理旧的注册信息
    NativeSerialization.getInstance().reset();
    synchronized (platforms) {
      // 遍历所有平台实现，逐个初始化
      for (Platform platform : platforms) {
        platform.init();
      }
    }
  }

  /**
   * 检查是否有平台支持指定键类型的原生序列化
   * @param keyClassName 键类全限定名
   * @param serializer 原生序列化器
   * @param job 作业配置对象
   * @return 如果有平台支持返回true，否则返回false
   */
  public static boolean support(String keyClassName,
      INativeSerializer<?> serializer, JobConf job) {
    synchronized (platforms) {
      // 遍历所有平台查找支持当前键类型的实现
      for (Platform platform : platforms) {
        if (platform.support(keyClassName, serializer, job)) {
          LOG.debug("platform " + platform.name() + " support key class"
            + keyClassName);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * 检查并定义指定键比较器的原生实现
   * @param keyComparator 键比较器类
   * @return 如果有平台定义了该比较器的原生实现返回true，否则返回false
   */
  public static boolean define(Class<?> keyComparator) {
    synchronized (platforms) {
      // 遍历所有平台查找定义了该比较器的实现
      for (Platform platform : platforms) {
        if (platform.define(keyComparator)) {
          LOG.debug("platform " + platform.name() + " define comparator "
            + keyComparator.getName());
          return true;
        }
      }
    }
    return false;
  }
}