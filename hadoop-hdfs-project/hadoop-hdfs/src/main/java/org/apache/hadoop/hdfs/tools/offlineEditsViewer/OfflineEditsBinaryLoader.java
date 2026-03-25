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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsViewer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.io.IOUtils;

/**
 * 二进制格式编辑日志文件加载器，用于离线编辑日志查看工具从二进制格式的edits文件中加载操作记录
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OfflineEditsBinaryLoader implements OfflineEditsLoader {
  private final OfflineEditsVisitor visitor;
  private final EditLogInputStream inputStream;
  private final boolean fixTxIds;
  private final boolean recoveryMode;
  private long nextTxId;
  public static final Logger LOG =
      LoggerFactory.getLogger(OfflineEditsBinaryLoader.class.getName());
  
  /**
   * 构造二进制编辑日志加载器，初始化加载所需配置与组件
   * @param visitor 访问器，用于处理解析出的每个编辑操作
   * @param inputStream 二进制编辑日志输入流
   * @param flags 离线编辑日志查看器的配置标志
   */
  public OfflineEditsBinaryLoader(OfflineEditsVisitor visitor,
        EditLogInputStream inputStream, OfflineEditsViewer.Flags flags) {
    this.visitor = visitor;
    this.inputStream = inputStream;
    this.fixTxIds = flags.getFixTxIds();
    this.recoveryMode = flags.getRecoveryMode();
    this.nextTxId = -1;
  }

  /**
   * 加载二进制编辑日志文件，逐个读取操作并调用访问器处理所有编辑记录
   */
  @Override
  public void loadEdits() throws IOException {
    try {
      // 启动访问器，传入编辑日志版本号
      visitor.start(inputStream.getVersion(true));
      // 循环读取所有操作直到文件结束
      while (true) {
        try {
          // 从输入流读取一个编辑操作
          FSEditLogOp op = inputStream.readOp();
          // 读到文件末尾，退出循环
          if (op == null)
            break;
          // 如果需要修正事务ID，对操作重新分配连续的事务ID
          if (fixTxIds) {
            // 初始化起始事务ID
            if (nextTxId <= 0) {
              nextTxId = op.getTransactionId();
              // 如果原事务ID不合法，从1开始编号
              if (nextTxId <= 0) {
                nextTxId = 1;
              }
            }
            // 设置修正后的事务ID，自增下一个ID
            op.setTransactionId(nextTxId);
            nextTxId++;
          }
          // 调用访问器处理当前编辑操作
          visitor.visitOp(op);
        } catch (IOException e) {
          // 非恢复模式下，清理后抛出异常
          if (!recoveryMode) {
            // Tell the visitor to clean up, then re-throw the exception
            LOG.error("Got IOException at position " +
              inputStream.getPosition());
            visitor.close(e);
            throw e;
          }
          // 恢复模式下，记录错误并重同步输入流跳过错误位置，继续读取
          LOG.error("Got IOException while reading stream!  Resyncing.", e);
          inputStream.resync();
        } catch (RuntimeException e) {
          // 非恢复模式下，清理后抛出异常
          if (!recoveryMode) {
            // Tell the visitor to clean up, then re-throw the exception
            LOG.error("Got RuntimeException at position " +
              inputStream.getPosition());
            visitor.close(e);
            throw e;
          }
          // 恢复模式下，记录错误并重同步输入流跳过错误位置，继续读取
          LOG.error("Got RuntimeException while reading stream!  Resyncing.", e);
          inputStream.resync();
        }
      }
      // 所有操作处理完成，正常关闭访问器
      visitor.close(null);
    } finally {
      // 最终清理：关闭输入流，释放资源
      IOUtils.cleanupWithLogger(LOG, inputStream);
    }
  }
}