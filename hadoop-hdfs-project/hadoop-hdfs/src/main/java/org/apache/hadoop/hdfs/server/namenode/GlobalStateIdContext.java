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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * 文件：全局状态ID上下文实现，HDFS Observer NameNode状态对齐机制的服务端核心组件
 * 功能：负责在HDFS HA架构下，处理客户端与Observer NameNode之间的状态ID对齐校验，
 *       保证读取请求能够拿到满足一致性要求的数据，当Observer落后过多时返回可重试异常让客户端重试
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class GlobalStateIdContext implements AlignmentContext {
  /**
   * 估计值：NameNode每秒可处理的日志事务数，用于估算Observer追赶上客户端状态ID需要的等待时间
   */
  private static final long ESTIMATED_TRANSACTIONS_PER_SECOND = 10000L;

  /**
   * 估算系数：服务端执行时间占客户端总等待时间的预期比例，用于计算可容忍的最大事务落后量
   */
  private static final float ESTIMATED_SERVER_TIME_MULTIPLIER = 0.8f;

  private final FSNamesystem namesystem;
  private final HashSet<String> coordinatedMethods;

  /**
   * 构造服务端全局状态ID上下文，初始化需要状态对齐的方法集合
   * @param namesystem FSNamesystem引用，用于获取当前NameNode状态和事务ID
   */
  GlobalStateIdContext(FSNamesystem namesystem) {
    this.namesystem = namesystem;
    this.coordinatedMethods = new HashSet<>();
    // 目前仅对ClientProtocol的方法进行状态对齐校验，因此只扫描该接口的方法
    for (Method method : ClientProtocol.class.getDeclaredMethods()) {
      // 收集带有@ReadOnly注解且开启了状态对齐协调的方法
      if (method.isAnnotationPresent(ReadOnly.class) &&
          method.getAnnotationsByType(ReadOnly.class)[0].isCoordinated()) {
        coordinatedMethods.add(method.getName());
      }
    }
  }

  /**
   * 更新RPC响应头中的状态ID，将当前NameNode最新的事务ID返回给客户端
   */
  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // 此处需要获取FSEditLog锁保证获取到正确的当前状态ID，未来可针对Observer场景优化
    header.setStateId(getLastSeenStateId());
  }

  /**
   * 服务端不需要处理响应状态，此方法为空实现
   */
  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Do nothing.
  }

  /**
   * 服务端不需要构建请求状态，此方法为空实现
   */
  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    // Do nothing.
  }

  /**
   * 处理客户端请求携带的状态ID，进行状态对齐校验，判断当前Observer是否能够满足读取一致性要求
   * 如果是Observer且落后客户端过多，抛出可重试异常让客户端重试；否则返回客户端状态ID继续处理请求
   * @param header RPC请求头，包含客户端携带的状态ID
   * @param clientWaitTime 客户端预计等待响应的最大时间，单位毫秒，用于计算可容忍的最大落后量
   * @return 校验通过后返回客户端状态ID，异常情况返回服务端状态ID
   * @throws RetriableException 当Observer状态落后超过可容忍范围，抛出该异常让客户端重试
   * @throws StandbyException 当Observer收到未携带状态ID的请求，抛出异常让客户端故障转移到Active
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header,
      long clientWaitTime) throws IOException {
    if (!header.hasStateId() &&
        HAServiceState.OBSERVER.equals(namesystem.getState())) {
      // 客户端未配置Observer代理时会出现这种情况，直接抛出异常让客户端故障转移到Active，避免返回 stale 数据
      throw new StandbyException("Observer Node received request without "
          + "stateId. This mostly likely is because client is not configured "
          + "with " + ObserverReadProxyProvider.class.getSimpleName());
    }
    // 获取当前服务端最新状态ID
    long serverStateId = getLastSeenStateId();
    // 获取客户端请求携带的状态ID
    long clientStateId = header.getStateId();
    FSNamesystem.LOG.trace("Client State ID= {} and Server State ID= {}",
        clientStateId, serverStateId);

    // 异常情况处理：Active节点的状态居然比客户端旧，记录警告并使用服务端状态ID
    if (clientStateId > serverStateId &&
        HAServiceState.ACTIVE.equals(namesystem.getState())) {
      FSNamesystem.LOG.warn("The client stateId: {} is greater than "
          + "the server stateId: {} This is unexpected. "
          + "Resetting client stateId to server stateId",
          clientStateId, serverStateId);
      return serverStateId;
    }
    // Observer场景：计算可容忍的最大落后事务量，若超过则抛出可重试异常
    if (HAServiceState.OBSERVER.equals(namesystem.getState()) &&
        clientStateId - serverStateId >
        ESTIMATED_TRANSACTIONS_PER_SECOND
            * TimeUnit.MILLISECONDS.toSeconds(clientWaitTime)
            * ESTIMATED_SERVER_TIME_MULTIPLIER) {
      throw new RetriableException(
          "Observer Node is too far behind: serverStateId = "
              + serverStateId + " clientStateId = " + clientStateId);
    }
    // 校验通过，返回客户端状态ID继续处理请求
    return clientStateId;
  }

  /**
   * 获取当前NameNode最新观测到的状态ID（即最后应用/写入的事务ID）
   */
  @Override
  public long getLastSeenStateId() {
    // 此处不需要调用getCorrectLastAppliedOrWrittenTxId，详见HDFS-14822
    return namesystem.getFSImage().getLastAppliedOrWrittenTxId();
  }

  /**
   * 判断指定RPC方法是否需要进行状态对齐协调
   * @param protocolName 协议名称
   * @param methodName 方法名称
   * @return true 如果该方法属于ClientProtocol且需要状态对齐，否则返回false
   */
  @Override
  public boolean isCoordinatedCall(String protocolName, String methodName) {
    return protocolName.equals(ClientProtocol.class.getCanonicalName())
        && coordinatedMethods.contains(methodName);
  }
}