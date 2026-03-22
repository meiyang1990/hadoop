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

import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;

/**
 * BPServiceActor执行动作的接口定义
 * 该接口由BPOfferService发起，用于命令BPServiceActor向NameNode执行上报等操作，
 * 是DataNode和NameNode块汇报心跳流程中可扩展动作的抽象基类。
 */
public interface BPServiceActorAction {
  /**
   * 向NameNode执行指定上报动作
   * @param bpNamenode 指向NameNode的Protocol Buffer客户端代理
   * @param bpRegistration 当前DataNode的注册信息
   * @throws BPServiceActorActionException 执行动作失败时抛出异常
   */
  public void reportTo(DatanodeProtocolClientSideTranslatorPB bpNamenode,
    DatanodeRegistration bpRegistration) throws BPServiceActorActionException;
}