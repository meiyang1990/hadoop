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
package org.apache.hadoop.mapreduce.server.tasktracker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * MapReduce TaskTracker服务端配置项常量接口，保存所有TaskTracker相关的配置键定义
 * 原为旧版MapReduce架构中TaskTracker的专用配置类，继承MR通用配置
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TTConfig extends MRConfig {

  /**
   *  @deprecated 请改用 {@link org.apache.hadoop.mapreduce.MRJobConfig#SHUFFLE_INDEX_CACHE}
   *   shuffle索引缓存内存配置键，单位MB
   */
  @Deprecated
  public static final String TT_INDEX_CACHE = 
    "mapreduce.tasktracker.indexcache.mb";
  
  /**
   * TaskTracker上最大Map任务槽数量配置键，控制同时运行的Map任务数
   */
  public static final String TT_MAP_SLOTS = 
    "mapreduce.tasktracker.map.tasks.maximum";
  
  /**
   * TaskTracker资源计算器插件类配置键，用于扩展自定义资源计算逻辑
   */
  public static final String TT_RESOURCE_CALCULATOR_PLUGIN = 
    "mapreduce.tasktracker.resourcecalculatorplugin";
}