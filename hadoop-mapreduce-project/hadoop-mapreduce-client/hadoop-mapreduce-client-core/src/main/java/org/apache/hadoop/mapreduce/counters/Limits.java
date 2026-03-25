// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.counters;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import static org.apache.hadoop.mapreduce.MRJobConfig.*;

/**
 * MapReduce计数器限额管理类，负责管理计数器、计数器组、名称长度的各类限额配置
 * 并提供限额检查能力，防止作业生成过多计数器导致内存占用过高
 */
@InterfaceAudience.Private
public class Limits {

  private int totalCounters;
  private LimitExceededException firstViolation;

  private static boolean isInited;
  
  private static int GROUP_NAME_MAX;
  private static int COUNTER_NAME_MAX;
  private static int GROUPS_MAX;
  private static int COUNTERS_MAX;
  
  /**
   * 从配置初始化计数器限额参数，懒加载模式保证只初始化一次
   * @param conf 配置对象，从中读取计数器限额配置
   */
  public synchronized static void init(Configuration conf) {
    if (!isInited) {
      if (conf == null) {
        conf = new JobConf();
      }
      // 读取计数器组名称最大长度配置
      GROUP_NAME_MAX = conf.getInt(COUNTER_GROUP_NAME_MAX_KEY,
          COUNTER_GROUP_NAME_MAX_DEFAULT);
      // 读取计数器名称最大长度配置
      COUNTER_NAME_MAX = conf.getInt(COUNTER_NAME_MAX_KEY,
          COUNTER_NAME_MAX_DEFAULT);
      // 读取计数器组最大数量配置
      GROUPS_MAX = conf.getInt(COUNTER_GROUPS_MAX_KEY, COUNTER_GROUPS_MAX_DEFAULT);
      // 读取计数器总最大数量配置
      COUNTERS_MAX = conf.getInt(COUNTERS_MAX_KEY, COUNTERS_MAX_DEFAULT);
    }
    isInited = true;
  }
  
  /**
   * 获取计数器组名称允许的最大长度，未初始化则自动初始化
   * @return 计数器组名称最大长度
   */
  public static int getGroupNameMax() {
    if (!isInited) {
      init(null);
    }
    return GROUP_NAME_MAX;
  }
  
  /**
   * 获取计数器名称允许的最大长度，未初始化则自动初始化
   * @return 计数器名称最大长度
   */
  public static int getCounterNameMax() {
    if (!isInited) {
      init(null);
    }
    return COUNTER_NAME_MAX;
  }
  
  /**
   * 获取计数器组允许的最大数量，未初始化则自动初始化
   * @return 计数器组最大数量
   */
  public static int getGroupsMax() {
    if (!isInited) {
      init(null);
    }
    return GROUPS_MAX;
  }
  
  /**
   * 获取计数器允许的最大总数量，未初始化则自动初始化
   * @return 计数器最大总数量
   */
  public static int getCountersMax() {
    if (!isInited) {
      init(null);
    }
    return COUNTERS_MAX;
  }
  
  /**
   * 根据最大长度过滤名称，超出长度则截断
   * @param name 原始名称字符串
   * @param maxLen 允许的最大长度
   * @return 过滤后的名称，超长则截断返回
   */
  public static String filterName(String name, int maxLen) {
    return name.length() > maxLen ? name.substring(0, maxLen - 1) : name;
  }

  /**
   * 按全局配置截断计数器名称
   * @param name 原始计数器名称
   * @return 截断后的计数器名称
   */
  public static String filterCounterName(String name) {
    return filterName(name, getCounterNameMax());
  }

  /**
   * 按全局配置截断计数器组名称
   * @param name 原始计数器组名称
   * @return 截断后的计数器组名称
   */
  public static String filterGroupName(String name) {
    return filterName(name, getGroupNameMax());
  }

  /**
   * 检查当前计数器数量是否超出限额，已超限则直接抛出异常
   * @param size 当前计数器总数量
   */
  public synchronized void checkCounters(int size) {
    if (firstViolation != null) {
      throw new LimitExceededException(firstViolation);
    }
    int countersMax = getCountersMax();
    if (size > countersMax) {
      firstViolation = new LimitExceededException("Too many counters: "+ size +
                                                  " max="+ countersMax);
      throw firstViolation;
    }
  }

  /**
   * 计数器数量自增前进行限额检查，通过后计数器计数+1
   */
  public synchronized void incrCounters() {
    checkCounters(totalCounters + 1);
    ++totalCounters;
  }

  /**
   * 检查计数器组数量是否超出限额，超出则记录异常不立即抛出
   * @param size 当前计数器组总数量
   */
  public synchronized void checkGroups(int size) {
    if (firstViolation != null) {
      throw new LimitExceededException(firstViolation);
    }
    int groupsMax = getGroupsMax();
    if (size > groupsMax) {
      firstViolation = new LimitExceededException("Too many counter groups: "+
                                                  size +" max="+ groupsMax);
    }
  }

  /**
   * 获取第一个限额超限异常对象
   * @return 第一个限额超限异常，无异常则返回null
   */
  public synchronized LimitExceededException violation() {
    return firstViolation;
  }
}