// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this writer except in compliance
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

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;

/**
 * 文件级注释：HDFS离线编辑日志查看工具的加载器接口，定义加载编辑日志并使用访问者处理的统一抽象
 * 
 * OfflineEditsLoader walks an EditsVisitor over an EditLogInputStream
 * 该接口定义了加载编辑日志，并使用访问者遍历处理日志条目统一行为
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface OfflineEditsLoader {
  /**
   * 加载编辑日志，并使用访问者遍历处理所有日志条目
   * 
   * @throws IOException 加载或处理过程中IO异常
   */
  abstract public void loadEdits() throws IOException;
  
  /**
   * 离线编辑日志加载器工厂类，根据输入文件类型创建对应格式的加载器实例
   */
  static class OfflineEditsLoaderFactory {
    /**
     * 根据输入文件类型和参数创建对应格式的离线编辑日志加载器
     * 
     * @param visitor 日志条目访问器，用于处理遍历到的日志条目
     * @param inputFileName 输入编辑日志文件路径
     * @param xmlInput 是否为XML格式的输入文件
     * @param flags 离线编辑日志查看器的配置参数
     * @return 对应格式的加载器实例
     * @throws IOException 创建过程中IO异常
     */
    static OfflineEditsLoader createLoader(OfflineEditsVisitor visitor,
        String inputFileName, boolean xmlInput,
        OfflineEditsViewer.Flags flags) throws IOException {
      if (xmlInput) {
        return new OfflineEditsXmlLoader(visitor, new File(inputFileName), flags);
      } else {
        File file = null;
        EditLogInputStream elis = null;
        OfflineEditsLoader loader = null;
        try {
          file = new File(inputFileName);
          // 打开二进制格式的编辑日志输入流，从起始到结束读取所有事务
          elis = new EditLogFileInputStream(file, HdfsServerConstants.INVALID_TXID,
              HdfsServerConstants.INVALID_TXID, false);
          // 创建二进制格式编辑日志加载器
          loader = new OfflineEditsBinaryLoader(visitor, elis, flags);
        } finally {
          // 创建失败时关闭已打开的输入流，避免资源泄漏
          if ((loader == null) && (elis != null)) {
            elis.close();
          }
        }
        return loader;
      }
    }
  }
}