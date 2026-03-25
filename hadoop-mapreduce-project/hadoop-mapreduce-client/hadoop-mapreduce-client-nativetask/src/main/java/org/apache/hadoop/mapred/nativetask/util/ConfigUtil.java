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
package org.apache.hadoop.mapred.nativetask.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 原生任务配置工具类，提供Hadoop配置对象到原生任务所需格式的转换工具方法
 * 为MapReduce本地原生任务模块提供配置序列化和格式转换能力
 */
@InterfaceAudience.Private
public abstract class ConfigUtil {
  
  /**
   * 将Hadoop Configuration配置对象转换为原生C++任务可识别的字节数组二维格式
   * 每个配置项会存储为两个连续字节数组：键、值，均采用UTF-8编码
   * @param conf Hadoop配置对象
   * @return 转换完成的二维字节数组，供原生任务读取配置
   */
  public static byte[][] toBytes(Configuration conf) {
    List<byte[]> nativeConfigs = new ArrayList<byte[]>();
    for (Map.Entry<String, String> e : conf) {
      nativeConfigs.add(e.getKey().getBytes(StandardCharsets.UTF_8));
      nativeConfigs.add(e.getValue().getBytes(StandardCharsets.UTF_8));
    }
    return nativeConfigs.toArray(new byte[nativeConfigs.size()][]);
  }
  
  /**
   * 将布尔数组转换为二进制字符串表示，true转换为'1'，false转换为'0'
   * @param value 输入布尔数组
   * @return 转换后的二进制字符串
   */
  public static String booleansToString(boolean[] value) {
    StringBuilder sb = new StringBuilder();
    for (boolean b: value) {
      sb.append(b ? 1 : 0);
    }
    return sb.toString();
  }
}