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

package org.apache.hadoop.mapreduce.lib.db;

import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件说明：Oracle数据库日期类型数据分片实现类，继承通用日期分片逻辑
 * 
 * 核心职责：针对Oracle数据库日期/时间类型字段实现分片分割逻辑，
 *          仅在生成分片条件语句时使用Oracle特定的SQL语法格式化日期，
 *          核心分片逻辑复用父类DateSplitter的通用实现。
 *          用于MapReduce并行从Oracle数据库读取数据时，按日期字段切分输入分片。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OracleDateSplitter extends DateSplitter {

  /**
   * 将Java Date对象转换为Oracle SQL可识别的时间戳字符串
   * @param d 待转换的Java日期对象
   * @return 符合Oracle语法的TO_TIMESTAMP函数调用字符串
   */
  @SuppressWarnings("unchecked")
  @Override
  protected String dateToString(Date d) {
    // Oracle日期对象实际都是Timestamp类型，使用TO_TIMESTAMP语法转换
    return "TO_TIMESTAMP('" + d.toString() + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
  }
}