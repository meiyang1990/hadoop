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

package org.apache.hadoop.yarn.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

/**
 * 应用 Masters 心跳请求异步处理线程，用于异步处理 AM 心跳请求，通过回调返回处理结果。
 */
public class AMHeartbeatRequestHandler extends SubjectInheritingThread {
  public static final Logger LOG =
      LoggerFactory.getLogger(AMHeartbeatRequestHandler.class);

  // 线程运行状态标记，控制线程是否继续工作
  private volatile boolean keepRunning;

  // 线程等待状态标记，供单元测试排空队列使用
  private volatile boolean isThreadWaiting;

  private Configuration conf;
  private ApplicationId applicationId;

  // 异步心跳请求队列，缓存待处理的分配请求
  private BlockingQueue<AsyncAllocateRequestInfo> requestQueue;
  // ResourceManager 客户端代理，负责转发心跳请求到RM
  private AMRMClientRelayer rmProxyRelayer;
  // 发起请求的用户凭据信息
  private UserGroupInformation userUgi;
  // 记录上一次心跳响应的ID，用于请求序号同步
  private int lastResponseId;

  /**
   * 构造AM心跳请求处理器，初始化线程和请求队列。
   * @param conf Hadoop配置对象
   * @param applicationId 所属应用ID
   * @param rmProxyRelayer RM客户端代理
   */
  public AMHeartbeatRequestHandler(Configuration conf,
      ApplicationId applicationId, AMRMClientRelayer rmProxyRelayer) {
    super("AMHeartbeatRequestHandler Heartbeat Handler Thread");
    this.setUncaughtExceptionHandler(
        new HeartBeatThreadUncaughtExceptionHandler());
    this.keepRunning = true;
    this.isThreadWaiting = false;

    this.conf = conf;
    this.applicationId = applicationId;
    this.requestQueue = new LinkedBlockingQueue<>();
    this.rmProxyRelayer = rmProxyRelayer;

    resetLastResponseId();
  }

  /**
   * 关闭该处理线程，终止心跳处理循环。
   */
  public void shutdown() {
    this.keepRunning = false;
    this.interrupt();
  }

  @Override
  public void work() {
    while (keepRunning) {
      AsyncAllocateRequestInfo requestInfo;
      try {
        // 标记线程进入等待状态
        this.isThreadWaiting = true;
        // 从队列阻塞取出待处理请求
        requestInfo = this.requestQueue.take();
        // 标记线程退出等待状态
        this.isThreadWaiting = false;

        if (requestInfo == null) {
          throw new YarnException(
              "Null requestInfo taken from request queue");
        }
        if (!this.keepRunning) {
          break;
        }

        // 在转发请求前设置响应ID，不同UAM可能需要独立序号
        AllocateRequest request = requestInfo.getRequest();
        if (request == null) {
          throw new YarnException("Null allocateRequest from requestInfo");
        }
        LOG.debug("Sending Heartbeat to RM. AskList:{}",
            ((request.getAskList() == null) ? " empty" :
            request.getAskList().size()));

        // 设置当前请求的响应ID
        request.setResponseId(lastResponseId);
        // 通过代理向RM发送分配请求（心跳）
        AllocateResponse response = rmProxyRelayer.allocate(request);
        if (response == null) {
          throw new YarnException("Null allocateResponse from allocate");
        }

        // 更新本地记录的最新响应ID
        lastResponseId = response.getResponseId();
        // 如果RM重新颁发了AMRM令牌，更新本地令牌
        if (response.getAMRMToken() != null) {
          LOG.debug("Received new AMRMToken");
          YarnServerSecurityUtils.updateAMRMToken(response.getAMRMToken(),
              userUgi, conf);
        }

        LOG.debug("Received Heartbeat reply from RM. Allocated Containers:{}",
            ((response.getAllocatedContainers() == null) ? " empty"
            : response.getAllocatedContainers().size()));

        if (requestInfo.getCallback() == null) {
          throw new YarnException("Null callback from requestInfo");
        }
        // 回调通知调用方处理结果
        requestInfo.getCallback().callback(response);
      } catch (InterruptedException ex) {
        LOG.debug("Interrupted while waiting for queue", ex);
      } catch (Throwable ex) {
        LOG.warn(
            "Error occurred while processing heart beat for " + applicationId,
            ex);
      }
    }

    LOG.info("AMHeartbeatRequestHandler thread for {} is exiting",
        applicationId);
  }

  /**
   * 重置响应ID为初始值0。
   */
  public void resetLastResponseId() {
    this.lastResponseId = 0;
  }

  /**
   * 设置连接RM使用的用户凭据UGI。
   * @param ugi 用户组信息对象
   */
  public void setUGI(UserGroupInformation ugi) {
    this.userUgi = ugi;
  }

  /**
   * 异步提交心跳分配请求，处理完成后通过回调返回结果。
   *
   * @param request 资源分配请求
   * @param callback 结果回调方法
   * @throws YarnException 如果请求入队失败
   */
  public void allocateAsync(AllocateRequest request,
      AsyncCallback<AllocateResponse> callback) throws YarnException {
    try {
      this.requestQueue.put(new AsyncAllocateRequestInfo(request, callback));
    } catch (InterruptedException ex) {
      // 队列长度无上限，理论上不会阻塞被中断
      LOG.debug("Interrupted while waiting to put on response queue", ex);
    }
  }

  /**
   * 排空请求队列，等待所有请求处理完成，仅用于单元测试。
   */
  @VisibleForTesting
  public void drainHeartbeatThread() {
    while (!this.isThreadWaiting || this.requestQueue.size() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * 获取当前请求队列长度，仅用于单元测试。
   */
  @VisibleForTesting
  public int getRequestQueueSize() {
    return this.requestQueue.size();
  }

  /**
   * 封装异步分配请求的结构体，保存请求对象和结果回调。
   */
  public static class AsyncAllocateRequestInfo {
    private AllocateRequest request;
    private AsyncCallback<AllocateResponse> callback;

    public AsyncAllocateRequestInfo(AllocateRequest request,
        AsyncCallback<AllocateResponse> callback) {
      Preconditions.checkArgument(request != null,
          "AllocateRequest cannot be null");
      Preconditions.checkArgument(callback != null, "Callback cannot be null");

      this.request = request;
      this.callback = callback;
    }

    public AsyncCallback<AllocateResponse> getCallback() {
      return this.callback;
    }

    public AllocateRequest getRequest() {
      return this.request;
    }
  }

  /**
   * 后台心跳线程的未捕获异常处理器，处理线程运行中未捕获的异常。
   */
  public class HeartBeatThreadUncaughtExceptionHandler
      implements UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.error("Heartbeat thread {} for application {} crashed!", t.getName(),
          applicationId, e);
    }
  }
}