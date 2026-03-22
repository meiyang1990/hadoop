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
package org.apache.hadoop.mapreduce.checkpoint;

/**
 * 简单实现的检查点命名服务，始终返回初始化时指定的固定名称（添加固定前缀）
 * 用于测试场景或不需要动态生成检查点名称的场景，实现CheckpointNamingService接口
 */
public class SimpleNamingService implements CheckpointNamingService{

  final String name;

  /**
   * 构造简单命名服务，使用指定名称生成检查点名称
   * @param name 基础名称，用于拼接生成最终检查点名称
   */
  public SimpleNamingService(String name){
    this.name = name;
  }

  /**
   * 生成检查点名称，返回固定格式的检查点名称
   * @return 拼接好的检查点名称，格式为checkpoint_<初始化时传入的名称>
   */
  public String getNewName(){
    return "checkpoint_" + name;
  }

}