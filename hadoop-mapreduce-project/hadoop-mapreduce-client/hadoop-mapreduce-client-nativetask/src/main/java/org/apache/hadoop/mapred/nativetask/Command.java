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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 原生任务命令实体类，用于封装原生任务执行过程中的命令标识和描述信息
 * 主要用于Java与Native任务执行框架之间的命令交互与标识区分
 */
@InterfaceAudience.Private
public class Command {

  private int id;
  private String description;

  /**
   * 构造只有ID的命令对象
   * @param id 命令唯一标识ID
   */
  public Command(int id) {
    this.id = id;
  }
  
  /**
   * 构造包含ID和描述的完整命令对象
   * @param id 命令唯一标识ID
   * @param description 命令描述信息
   */
  public Command(int id, String description) {
    this.id = id;
    this.description = description;
  }
  
  /**
   * 获取命令唯一标识ID
   * @return 命令ID
   */
  public int id() {
    return this.id;
  }
  
  /**
   * 获取命令描述信息
   * @return 命令描述文本
   */
  public String description() {
    return this.description;
  }

  /**
   * 基于命令ID判断两个命令是否相等
   * @param other 待比较的对象
   * @return 相等返回true，否则返回false
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof Command) {
      return this.id == ((Command)other).id;
    }
    return false;
  }
  
  /**
   * 基于命令ID生成哈希码
   * @return 命令ID本身作为哈希码
   */
  @Override
  public int hashCode() {
    return id;
  }
}