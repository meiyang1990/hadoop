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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 通用标识符抽象基类，内部使用整数存储ID值，是MapReduce旧API中JobID、TaskID和TaskAttemptID的父类
 * 为MapReduce框架中各类实体提供统一的ID基础实现
 * 
 * @see JobID
 * @see TaskID
 * @see TaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ID extends org.apache.hadoop.mapreduce.ID {

  /**
   * 根据给定整数构造ID对象
   * @param id 整数形式的ID值
   */
  public ID(int id) {
    super(id);
  }

  /**
   * 空构造方法，供子类继承使用
   */
  protected ID() {
  }

}