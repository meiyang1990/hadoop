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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;

/**
 * 检查点流程故障注入工具类，用于为NameNode检查点过程的测试提供故障注入点，支持测试异常场景下的系统稳定性。
 * 可以通过继承此类重写方法，模拟不同阶段的故障，测试检查点流程的容错能力。
 */
public class CheckpointFaultInjector {
  /** 单例实例 */
  public static CheckpointFaultInjector instance =
      new CheckpointFaultInjector();

  /**
   * 获取故障注入器单例实例
   * @return 故障注入器实例
   */
  public static CheckpointFaultInjector getInstance() {
    return instance;
  }

  /**
   * 设置自定义故障注入器实例，用于替换默认实现注入自定义故障
   * @param instance 自定义故障注入器实例
   */
  public static void set(CheckpointFaultInjector instance) {
    CheckpointFaultInjector.instance = instance;
  }

  /**
   * 获取镜像集合头信息之前触发的故障点
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void beforeGetImageSetsHeaders() throws IOException {}

  /**
   * 备用节点完成编辑日志滚动之后触发的故障点
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void afterSecondaryCallsRollEditLog() throws IOException {}

  /**
   * 镜像合并过程中触发的故障点
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void duringMerge() throws IOException {}

  /**
   * 备用节点上传新镜像完成之后触发的故障点
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void afterSecondaryUploadsNewImage() throws IOException {}

  /**
   * 准备发送文件之前触发的故障点
   * @param localfile 待发送的本地文件
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void aboutToSendFile(File localfile) throws IOException {}

  /**
   * 判断是否应该发送截断的短文件，用于模拟文件传输不完整故障
   * @param localfile 待发送的本地文件
   * @return 是否发送短文件，默认返回false不模拟
   */
  public boolean shouldSendShortFile(File localfile) {
    return false;
  }

  /**
   * 判断是否应该损坏文件中的一个字节，用于模拟数据损坏故障
   * @param localfile 待处理的本地文件
   * @return 是否损坏字节，默认返回false不模拟
   */
  public boolean shouldCorruptAByte(File localfile) {
    return false;
  }
  
  /**
   * MD5文件重命名完成之后触发的故障点
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void afterMD5Rename() throws IOException {}

  /**
   * 编辑日志重命名之前触发的故障点
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void beforeEditsRename() throws IOException {}

  /**
   * 文件上传过程中触发的故障点，可模拟中断或延迟
   * @throws InterruptedException 可抛出中断异常模拟故障
   * @throws IOException 可抛出IO异常模拟故障
   */
  public void duringUploadInProgess() throws InterruptedException, IOException {
  }

}