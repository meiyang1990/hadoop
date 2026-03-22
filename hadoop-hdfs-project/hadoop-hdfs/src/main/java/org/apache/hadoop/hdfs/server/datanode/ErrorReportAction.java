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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.RemoteException;

/**
 * 文件级注释：错误上报动作类，封装需要向NameNode上报的错误信息，由BPOfferService生成并交给BPServiceActor执行上报操作
 * <p>
 * 该类实现了BPServiceActorAction接口，是DataNode向NameNode上报错误的可执行动作单元，
 * 用于将数据节点本地检测到的块相关错误等异常信息上报给NameNode处理。
 */
public class ErrorReportAction implements BPServiceActorAction {

  final int errorCode;
  final String errorMessage;
  
  /**
   * 构造错误上报动作对象，初始化错误码和错误信息
   * @param errorCode 错误类型编码
   * @param errorMessage 错误详情描述
   */
  public ErrorReportAction(int errorCode, String errorMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }
  
  /**
   * 执行错误上报动作，将封装的错误信息上报给NameNode
   * @param bpNamenode NameNode协议客户端代理，用于向NameNode发起RPC调用
   * @param bpRegistration 当前DataNode的注册信息
   * @throws BPServiceActorActionException 上报失败时抛出异常
   */
  @Override
  public void reportTo(DatanodeProtocolClientSideTranslatorPB bpNamenode, 
    DatanodeRegistration bpRegistration) throws BPServiceActorActionException {
    try {
      // 调用NameNode RPC接口上报错误信息
      bpNamenode.errorReport(bpRegistration, errorCode, errorMessage);
    } catch (RemoteException re) {
      // 捕获NameNode返回的远程异常，仅记录日志不中断流程
      DataNode.LOG.info("trySendErrorReport encountered RemoteException  "
          + "errorMessage: " + errorMessage + "  errorCode: " + errorCode, re);
    } catch(IOException e) {
      // IO异常封装为动作执行异常抛出
      throw new BPServiceActorActionException("Error reporting "
          + "an error to namenode.", e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + errorCode;
    result = prime * result
        + ((errorMessage == null) ? 0 : errorMessage.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ErrorReportAction)) {
      return false;
    }
    ErrorReportAction other = (ErrorReportAction) obj;
    if (errorCode != other.errorCode) {
      return false;
    }
    if (errorMessage == null) {
      if (other.errorMessage != null) {
        return false;
      }
    } else if (!errorMessage.equals(other.errorMessage)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("errorCode", errorCode)
        .append("errorMessage", errorMessage)
        .toString();
  }
}