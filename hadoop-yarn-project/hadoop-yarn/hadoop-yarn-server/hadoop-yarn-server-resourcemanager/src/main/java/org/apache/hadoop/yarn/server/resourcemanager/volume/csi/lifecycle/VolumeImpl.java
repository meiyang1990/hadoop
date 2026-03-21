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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeCapability;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.VolumeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.VolumeEventType;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.AccessMode.SINGLE_NODE_READER_ONLY;
import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeType.FILE_SYSTEM;

/**
 * 文件级注释：YARN CSI存储卷生命周期实现类，维护存储卷状态并处理状态转换，符合CSI存储生命周期规范
 * 存储卷状态定义在{@link org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeStates}
 */
public class VolumeImpl implements Volume {

  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeImpl.class);

  private final Lock readLock;
  private final Lock writeLock;
  // 存储卷状态机，管理状态与事件转换
  private final StateMachine<VolumeState, VolumeEventType, VolumeEvent>
      stateMachine;

  private final VolumeId volumeId;
  private final VolumeMetaData volumeMeta;
  // CSI适配器客户端，用于与CSI适配器服务通信
  private CsiAdaptorProtocol adaptorClient;

  /**
   * 构造函数：初始化CSI存储卷实例
   * @param volumeMeta 存储卷元数据
   */
  public VolumeImpl(VolumeMetaData volumeMeta) {
    ReadWriteLock lock = new ReentrantReadWriteLock();
    this.writeLock = lock.writeLock();
    this.readLock = lock.readLock();
    this.volumeId = volumeMeta.getVolumeId();
    this.volumeMeta = volumeMeta;
    this.stateMachine = createVolumeStateFactory().make(this);
  }

  @VisibleForTesting
  public void setClient(CsiAdaptorProtocol csiAdaptorClient) {
    this.adaptorClient = csiAdaptorClient;
  }

  @Override
  public CsiAdaptorProtocol getClient() {
    return this.adaptorClient;
  }

  @Override
  public VolumeMetaData getVolumeMeta() {
    return this.volumeMeta;
  }

  /**
   * 创建存储卷状态机工厂，定义所有合法状态转换规则
   * @return 配置完成的状态机工厂
   */
  private StateMachineFactory<VolumeImpl, VolumeState,
      VolumeEventType, VolumeEvent> createVolumeStateFactory() {
    return new StateMachineFactory<
        VolumeImpl, VolumeState, VolumeEventType, VolumeEvent>(VolumeState.NEW)
        // NEW状态处理验证事件，可转换为VALIDATED或UNAVAILABLE
        .addTransition(
            VolumeState.NEW,
            EnumSet.of(VolumeState.VALIDATED, VolumeState.UNAVAILABLE),
            VolumeEventType.VALIDATE_VOLUME_EVENT,
            new ValidateVolumeTransition())
        // VALIDATED状态重复验证，保持状态不变
        .addTransition(VolumeState.VALIDATED, VolumeState.VALIDATED,
            VolumeEventType.VALIDATE_VOLUME_EVENT)
        // VALIDATED状态处理发布事件，可转换为NODE_READY或UNAVAILABLE
        .addTransition(
            VolumeState.VALIDATED,
            EnumSet.of(VolumeState.NODE_READY, VolumeState.UNAVAILABLE),
            VolumeEventType.CONTROLLER_PUBLISH_VOLUME_EVENT,
            new ControllerPublishVolumeTransition())
        // UNAVAILABLE状态重新验证，可保持UNAVAILABLE或转为VALIDATED
        .addTransition(
            VolumeState.UNAVAILABLE,
            EnumSet.of(VolumeState.UNAVAILABLE, VolumeState.VALIDATED),
            VolumeEventType.VALIDATE_VOLUME_EVENT,
            new ValidateVolumeTransition())
        // UNAVAILABLE状态处理发布事件，保持UNAVAILABLE不变
        .addTransition(
            VolumeState.UNAVAILABLE,
            VolumeState.UNAVAILABLE,
            EnumSet.of(VolumeEventType.CONTROLLER_PUBLISH_VOLUME_EVENT))
        // NODE_READY状态处理验证和发布事件，保持状态不变
        .addTransition(
            VolumeState.NODE_READY,
            VolumeState.NODE_READY,
            EnumSet.of(VolumeEventType.CONTROLLER_PUBLISH_VOLUME_EVENT,
                VolumeEventType.VALIDATE_VOLUME_EVENT))
        .installTopology();
  }

  @Override
  public VolumeState getVolumeState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public VolumeId getVolumeId() {
    readLock.lock();
    try {
      return this.volumeId;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 存储卷验证状态转换处理，调用CSI适配器验证存储卷能力
   */
  private static class ValidateVolumeTransition
      implements MultipleArcTransition<VolumeImpl, VolumeEvent, VolumeState> {
    @Override
    public VolumeState transition(VolumeImpl volume,
        VolumeEvent volumeEvent) {
      // 部分CSI驱动未实现验证能力，当前直接处理不抛出异常
      try {
        // 跨节点调用保持消息精简，TODO: 后续从存储卷资源规格解析能力
        // 构造默认存储卷能力：单节点只读、文件系统类型
        VolumeCapability capability = new VolumeCapability(
            SINGLE_NODE_READER_ONLY, FILE_SYSTEM,
            ImmutableList.of());
        // 构造验证请求
        ValidateVolumeCapabilitiesRequest request =
            ValidateVolumeCapabilitiesRequest
                .newInstance(volume.getVolumeId().getId(),
                    ImmutableList.of(capability),
                    ImmutableMap.of());
        // 调用CSI适配器验证存储卷
        ValidateVolumeCapabilitiesResponse response = volume.getClient()
            .validateVolumeCapacity(request);
        // 根据验证结果返回目标状态
        return response.isSupported() ? VolumeState.VALIDATED
            : VolumeState.UNAVAILABLE;
      } catch (YarnException | IOException e) {
        // 验证调用异常，标记存储卷不可用
        LOG.warn("Got exception while calling the CSI adaptor", e);
        return VolumeState.UNAVAILABLE;
      }
    }
  }

  /**
   * 控制器发布存储卷状态转换处理
   */
  private static class ControllerPublishVolumeTransition
      implements MultipleArcTransition<VolumeImpl, VolumeEvent, VolumeState> {

    @Override
    public VolumeState transition(VolumeImpl volume,
        VolumeEvent volumeEvent) {
      // 跨节点调用保持消息精简，当前默认发布成功直接返回NODE_READY
      return VolumeState.NODE_READY;
    }
  }

  @Override
  public void handle(VolumeEvent event) {
    // 获取写锁保证状态转换线程安全
    this.writeLock.lock();
    try {
      VolumeId volumeId = event.getVolumeId();

      if (volumeId == null) {
        // 无效事件，日志警告后忽略
        LOG.warn("Unexpected volume event received, event type is "
            + event.getType().name() + ", but the volumeId is null.");
        return;
      }

      LOG.info("Processing volume event, type=" + event.getType().name()
          + ", volumeId=" + volumeId.toString());

      VolumeState oldState = null;
      VolumeState newState = null;
      try {
        // 获取转换前状态，执行状态转换
        oldState = stateMachine.getCurrentState();
        newState = stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        // 非法状态转换，日志警告后忽略
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]," +
            " volumeId: [" + volumeId + "]", e);
      }

      // 状态发生变更，打印日志记录
      if (newState != null && oldState != newState) {
        LOG.info("VolumeImpl " + volumeId + " transitioned from " + oldState
            + " to " + newState);
      }
    }finally {
      // 释放写锁
      this.writeLock.unlock();
    }
  }
}