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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * DefaultRequestInterceptor是AMRMProxy请求拦截链的最终实现，
 * 核心职责是将ApplicationMaster的请求直接转发给集群ResourceManager。
 * 作为拦截链的最后一环，不支持设置后续拦截器。
 */
public final class DefaultRequestInterceptor extends
    AbstractRequestInterceptor {
  private static final Logger LOG = LoggerFactory
      .getLogger(DefaultRequestInterceptor.class);
  // ResourceManager客户端代理
  private ApplicationMasterProtocol rmClient;
  // 代理用户，用于代表ApplicationMaster发起请求
  private UserGroupInformation user = null;

  @Override
  public void init(AMRMProxyApplicationContext appContext) {
    super.init(appContext);
    try {
      // 创建代表当前应用尝试的代理用户
      user =
          UserGroupInformation.createProxyUser(appContext
              .getApplicationAttemptId().toString(), UserGroupInformation
              .getCurrentUser());
      // 添加AMRM身份认证令牌
      user.addToken(appContext.getAMRMToken());
      final Configuration conf = this.getConf();

      // 创建ResourceManager客户端
      rmClient = createRMClient(appContext, conf);
    } catch (IOException e) {
      String message =
          "Error while creating of RM app master service proxy for attemptId:"
              + appContext.getApplicationAttemptId().toString();
      if (user != null) {
        message += ", user: " + user;
      }

      LOG.info(message);
      throw new YarnRuntimeException(message, e);
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * 根据是否启用分布式调度创建对应类型的RM客户端代理。
   * @param appContext AMRMProxy应用上下文
   * @param conf 配置信息
   * @return RM客户端代理实例
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  private ApplicationMasterProtocol createRMClient(
      AMRMProxyApplicationContext appContext, final Configuration conf)
      throws IOException, InterruptedException {
    if (appContext.getNMContext().isDistributedSchedulingEnabled()) {
      // 分布式调度模式，使用服务端RM代理创建分布式调度协议客户端
      return user.doAs((PrivilegedExceptionAction<DistributedSchedulingAMProtocol>) () -> {
        setAMRMTokenService(conf);
        return ServerRMProxy.createRMProxy(conf, DistributedSchedulingAMProtocol.class);
      });
    } else {
      // 普通调度模式，使用客户端RM代理创建标准AM协议客户端
      return user.doAs(
          (PrivilegedExceptionAction<ApplicationMasterProtocol>) () -> {
            setAMRMTokenService(conf);
            return ClientRMProxy.createRMProxy(conf,
                ApplicationMasterProtocol.class);
          });
    }
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      final RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    LOG.info("Forwarding registration request to the real YARN RM");
    // 转发注册请求到RM
    return rmClient.registerApplicationMaster(request);
  }

  @Override
  public AllocateResponse allocate(final AllocateRequest request)
      throws YarnException, IOException {
    LOG.debug("Forwarding allocate request to the real YARN RM");
    // 转发资源分配请求到RM
    AllocateResponse allocateResponse = rmClient.allocate(request);
    // 如果RM返回了新的AMRM令牌，更新当前用户的令牌信息
    if (allocateResponse.getAMRMToken() != null) {
      YarnServerSecurityUtils.updateAMRMToken(allocateResponse.getAMRMToken(),
          this.user, getConf());
    }

    return allocateResponse;
  }

  @Override
  public RegisterDistributedSchedulingAMResponse
  registerApplicationMasterForDistributedScheduling
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    // 检查分布式调度是否启用
    if (getApplicationContext().getNMContext()
        .isDistributedSchedulingEnabled()) {
      LOG.info("Forwarding registerApplicationMasterForDistributedScheduling" +
          "request to the real YARN RM");
      // 转发分布式调度注册请求到RM
      return ((DistributedSchedulingAMProtocol)rmClient)
          .registerApplicationMasterForDistributedScheduling(request);
    } else {
      throw new YarnException("Distributed Scheduling is not enabled.");
    }
  }

  @Override
  public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException {
    LOG.debug("Forwarding allocateForDistributedScheduling request" +
        "to the real YARN RM");
    // 检查分布式调度是否启用
    if (getApplicationContext().getNMContext()
        .isDistributedSchedulingEnabled()) {
      // 转发分布式调度分配请求到RM
      DistributedSchedulingAllocateResponse allocateResponse =
          ((DistributedSchedulingAMProtocol)rmClient)
              .allocateForDistributedScheduling(request);
      // 如果RM返回了新的AMRM令牌，更新当前用户的令牌信息
      if (allocateResponse.getAllocateResponse().getAMRMToken() != null) {
        YarnServerSecurityUtils.updateAMRMToken(
            allocateResponse.getAllocateResponse().getAMRMToken(), this.user,
            getConf());
      }
      return allocateResponse;
    } else {
      throw new YarnException("Distributed Scheduling is not enabled.");
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      final FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    LOG.info("Forwarding finish application request to "
        + "the real YARN Resource Manager");
    // 转发应用完成请求到RM
    return rmClient.finishApplicationMaster(request);
  }

  @Override
  public void setNextInterceptor(RequestInterceptor next) {
    // DefaultRequestInterceptor是拦截链最后一环，不允许设置后续拦截器，直接抛出异常
    throw new YarnRuntimeException(
        "setNextInterceptor is being called on DefaultRequestInterceptor,"
            + "which should be the last one in the chain "
            + "Check if the interceptor pipeline configuration is correct");
  }

  /**
   * 供测试用，设置RM客户端代理实例。
   * 自动适配非分布式调度客户端，包装为分布式调度协议实现。
   * @param rmClient RM客户端实例
   */
  @VisibleForTesting
  public void setRMClient(final ApplicationMasterProtocol rmClient) {
    if (rmClient instanceof DistributedSchedulingAMProtocol) {
      this.rmClient = rmClient;
    } else {
      // 包装普通RM客户端，实现分布式调度协议接口，不支持分布式调度方法
      this.rmClient = new DistributedSchedulingAMProtocol() {
        @Override
        public RegisterApplicationMasterResponse registerApplicationMaster
            (RegisterApplicationMasterRequest request) throws YarnException,
            IOException {
          return rmClient.registerApplicationMaster(request);
        }

        @Override
        public FinishApplicationMasterResponse finishApplicationMaster
            (FinishApplicationMasterRequest request) throws YarnException,
            IOException {
          return rmClient.finishApplicationMaster(request);
        }

        @Override
        public AllocateResponse allocate(AllocateRequest request) throws
            YarnException, IOException {
          return rmClient.allocate(request);
        }

        @Override
        public RegisterDistributedSchedulingAMResponse
        registerApplicationMasterForDistributedScheduling
            (RegisterApplicationMasterRequest request) throws YarnException,
            IOException {
          throw new IOException("Not Supported !!");
        }

        @Override
        public DistributedSchedulingAllocateResponse
            allocateForDistributedScheduling(
            DistributedSchedulingAllocateRequest request)
                throws YarnException, IOException {
          throw new IOException("Not Supported !!");
        }
      };
    }
  }

  /**
   * 更新当前用户AMRM令牌的服务地址，确保令牌指向正确的RM服务。
   * @param conf 配置信息
   * @throws IOException IO异常
   */
  private static void setAMRMTokenService(final Configuration conf)
      throws IOException {
    // 遍历当前用户所有令牌，更新AMRM令牌的服务地址
    for (org.apache.hadoop.security.token.Token<? extends TokenIdentifier> token : UserGroupInformation
        .getCurrentUser().getTokens()) {
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        token.setService(ClientRMProxy.getAMRMTokenService(conf));
      }
    }
  }

  /**
   * 根据配置构造AMRM令牌对应的服务标识，支持HA模式多RM场景。
   * HA模式下会拼接所有RM实例的服务地址作为令牌服务名。
   * @param conf 配置信息
   * @param address RM地址配置项
   * @param defaultAddr 默认地址
   * @param defaultPort 默认端口
   * @return 令牌服务标识
   */
  @InterfaceStability.Unstable
  public static Text getTokenService(Configuration conf, String address,
      String defaultAddr, int defaultPort) {
    if (HAUtil.isHAEnabled(conf)) {
      // HA模式，收集所有RM实例的服务地址
      ArrayList<String> services = new ArrayList<>();
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      for (String rmId : HAUtil.getRMHAIds(conf)) {
        // 设置当前RM ID，获取对应RM的地址
        yarnConf.set(YarnConfiguration.RM_HA_ID, rmId);
        services.add(SecurityUtil.buildTokenService(
            yarnConf.getSocketAddr(address, defaultAddr, defaultPort))
            .toString());
      }
      // 用逗号拼接所有RM服务地址
      return new Text(Joiner.on(',').join(services));
    }

    // 非HA模式，直接构造单RM服务标识
    return SecurityUtil.buildTokenService(conf.getSocketAddr(address,
        defaultAddr, defaultPort));
  }
}