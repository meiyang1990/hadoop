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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * 基于键字段规则的比较器，兼容旧版MapRed API，提供类似Unix/GNU Sort的排序能力
 * 支持的排序特性包括：
 * <ul>
 * <li>-n：按数值排序</li>
 * <li>-r：反转比较结果（降序排序）</li>
 * <li>-k pos1[,pos2]：指定排序基于的键字段范围，格式为 f[.c][opts]
 *   <ul>
 *   <li>f：字段编号，从1开始计数</li>
 *   <li>c：字段内起始/结束字符位置，从1开始计数；pos2中0表示字段最后一个字符</li>
 *   <li>pos1省略.c时默认从1（字段开头）开始，pos2省略.c时默认到0（字段结尾）结束</li>
 *   <li>opts：当前字段的排序选项，支持'n'和'r'</li>
 *   </ul>
 * </li>
 * </ul>
 * 键中字段的分隔符由 {@link JobContext#MAP_OUTPUT_KEY_FIELD_SEPARATOR} 配置指定
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * 基于键字段范围的排序比较器，兼容旧版MapRed API
 * 核心功能是根据用户指定的键中字段位置和排序规则，对Map输出键进行排序
 */
public class KeyFieldBasedComparator<K, V> extends 
    org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator<K, V>
    implements JobConfigurable {

  /**
   * 从旧版JobConf配置比较器参数，调用父类完成初始化
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    super.setConf(job);
  }
}