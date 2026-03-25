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
package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * MapReduce溢写过程回调注入器，用于测试场景注入自定义回调处理，生产环境调用为空操作。
 * 核心作用是在单元测试中拦截溢写文件操作，实现溢写文件损坏、异常注入等测试场景。
 */
@VisibleForTesting
@InterfaceAudience.Private
public class SpillCallBackInjector {
  private static SpillCallBackInjector instance = new SpillCallBackInjector();

  /**
   * 获取全局单例的溢写回调注入器实例。
   * @return 全局注入器实例
   */
  public static SpillCallBackInjector get() {
    return instance;
  }

  /**
   * 更新全局溢写回调注入器实例，返回原实例。用于测试时替换自定义注入实现。
   *
   * @param spillInjector 新的溢写回调注入器实现
   * @return 替换前的原注入器实例
   */
  public static SpillCallBackInjector getAndSet(
      SpillCallBackInjector spillInjector) {
    SpillCallBackInjector prev = instance;
    instance = spillInjector;
    return prev;
  }

  /**
   * 写入溢写索引文件完成回调。
   * @param path 溢写索引文件路径
   */
  public void writeSpillIndexFileCB(Path path) {
    // do nothing
  }

  /**
   * 写入溢写数据文件完成回调。
   * @param path 溢写数据文件路径
   * @param out 溢写文件输出流
   * @param conf Hadoop配置对象
   */
  public void writeSpillFileCB(Path path, FSDataOutputStream out,
      Configuration conf) {
    // do nothing
  }

  /**
   * 读取溢写数据文件回调。
   * @param path 溢写数据文件路径
   * @param is 溢写文件输入流
   * @param conf Hadoop配置对象
   */
  public void getSpillFileCB(Path path, InputStream is, Configuration conf) {
    // do nothing
  }

  /**
   * 获取溢写文件操作报告，用于测试结果校验。
   * @return 溢写操作报告字符串，默认返回null
   */
  public String getSpilledFileReport() {
    return null;
  }

  /**
   * 溢写填充过程异常处理回调。
   * @param path 发生异常的溢写文件路径
   * @param e 捕获到的异常对象
   */
  public void handleErrorInSpillFill(Path path, Exception e) {
    // do nothing
  }

  /**
   * 损坏指定溢写文件，供测试异常场景使用。
   * @param fileName 待损坏的溢写文件路径
   * @throws IOException 操作文件时发生IO异常
   */
  public void corruptSpilledFile(Path fileName) throws IOException {
    // do nothing
  }

  /**
   * 添加溢写索引文件完成回调。
   * @param path 溢写索引文件路径
   * @param conf Hadoop配置对象
   */
  public void addSpillIndexFileCB(Path path, Configuration conf) {
    // do nothing
  }

  /**
   * 校验溢写索引文件回调。
   * @param path 待校验的溢写索引文件路径
   * @param conf Hadoop配置对象
   */
  public void validateSpillIndexFileCB(Path path, Configuration conf) {
    // do nothing
  }
}