// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * 日志Web服务工具类，为YARN日志查询Web服务提供通用工具能力
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class LogWebServiceUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(LogWebServiceUtils.class);

  private LogWebServiceUtils() {
  }

  private static final Joiner DOT_JOINER = Joiner.on(". ");

  /**
   * 构造流式日志响应，将聚合日志以流式方式返回给请求方
   * @param factory 日志聚合文件控制器工厂
   * @param appId 应用ID
   * @param appOwner 应用拥有者
   * @param nodeId 节点ID
   * @param containerIdStr 容器ID字符串
   * @param fileName 日志文件名
   * @param format 返回格式
   * @param bytes 限制返回的字节数，0表示不限制
   * @param printEmptyLocalContainerLog 是否打印空本地容器日志提示
   * @return 流式响应对象
   */
  public static Response sendStreamOutputResponse(
      LogAggregationFileControllerFactory factory, ApplicationId appId,
      String appOwner, String nodeId, String containerIdStr, String fileName,
      String format, long bytes, boolean printEmptyLocalContainerLog) {
    String contentType = WebAppUtils.getDefaultLogContentType();
    // 如果指定了格式，设置对应Content-Type
    if (format != null && !format.isEmpty()) {
      contentType = WebAppUtils.getSupportedLogContentType(format);
      if (contentType == null) {
        String errorMessage =
            "The valid values for the parameter : format " + "are "
                + WebAppUtils.listSupportedLogContentType();
        return Response.status(Response.Status.BAD_REQUEST).entity(errorMessage)
            .build();
      }
    }
    StreamingOutput stream = null;
    try {
      // 获取日志流输出对象
      stream =
          getStreamingOutput(factory, appId, appOwner, nodeId, containerIdStr,
              fileName, bytes, printEmptyLocalContainerLog);
    } catch (Exception ex) {
      LOG.debug("Exception", ex);
      return createBadResponse(Response.Status.INTERNAL_SERVER_ERROR,
          ex.getMessage());
    }
    // 构建响应对象，设置响应头
    Response.ResponseBuilder response = Response.ok(stream);
    response.header("Content-Type", contentType);
    // Sending the X-Content-Type-Options response header with the value
    // nosniff will prevent Internet Explorer from MIME-sniffing a response
    // away from the declared content-type.
    response.header("X-Content-Type-Options", "nosniff");
    return response.build();
  }

  /**
   * 创建StreamingOutput实现，从日志聚合存储中读取日志并写入输出流
   */
  private static StreamingOutput getStreamingOutput(
      final LogAggregationFileControllerFactory factory,
      final ApplicationId appId, final String appOwner, final String nodeId,
      final String containerIdStr, final String logFile, final long bytes,
      final boolean printEmptyLocalContainerLog) throws IOException {
    StreamingOutput stream = new StreamingOutput() {

      @Override public void write(OutputStream os)
          throws IOException, WebApplicationException {
        // 构建容器日志请求对象
        ContainerLogsRequest request = new ContainerLogsRequest();
        request.setAppId(appId);
        request.setAppOwner(appOwner);
        request.setContainerId(containerIdStr);
        request.setBytes(bytes);
        request.setNodeId(nodeId);
        Set<String> logTypes = new HashSet<>();
        logTypes.add(logFile);
        request.setLogTypes(logTypes);
        // 读取聚合日志到输出流
        boolean findLogs = factory.getFileControllerForRead(appId, appOwner)
            .readAggregatedLogs(request, os);
        // 未找到日志返回提示信息
        if (!findLogs) {
          os.write(("Can not find logs for container:" + containerIdStr)
              .getBytes(StandardCharsets.UTF_8));
        } else {
          // 需要打印本地日志提示时，输出重定向警告信息
          if (printEmptyLocalContainerLog) {
            StringBuilder sb = new StringBuilder();
            sb.append(containerIdStr + "\n");
            sb.append("LogAggregationType: " + ContainerLogAggregationType.LOCAL
                + "\n");
            sb.append("LogContents:\n");
            sb.append(getNoRedirectWarning() + "\n");
            os.write(sb.toString().getBytes(StandardCharsets.UTF_8));
          }
        }
      }
    };
    return stream;
  }

  /**
   * 获取无法重定向到NodeManager获取本地日志的警告信息
   * @return 警告文本
   */
  public static String getNoRedirectWarning() {
    return "We do not have NodeManager web address, so we can not "
        + "re-direct the request to related NodeManager "
        + "for local container logs.";
  }

  /**
   * 重新包装并抛出异常，根据异常类型转换为对应Web异常
   * @param e 原始异常
   */
  public static void rewrapAndThrowException(Exception e) {
    if (e instanceof UndeclaredThrowableException) {
      rewrapAndThrowThrowable(e.getCause());
    } else {
      rewrapAndThrowThrowable(e);
    }
  }

  /**
   * 重新包装并抛出Throwable，认证异常转为ForbiddenException，其他转为WebApplicationException
   * @param t 原始Throwable
   */
  public static void rewrapAndThrowThrowable(Throwable t) {
    if (t instanceof AuthorizationException) {
      throw new ForbiddenException(t);
    } else {
      throw new WebApplicationException(t);
    }
  }

  /**
   * 解析长度参数，空参数返回Long.MAX_VALUE（表示不限制）
   * @param bytes 参数字符串
   * @return 解析后的长度值
   */
  public static long parseLongParam(String bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return Long.MAX_VALUE;
    }
    return Long.parseLong(bytes);
  }

  /**
   * 创建错误响应，封装状态码和错误信息
   * @param status HTTP状态码
   * @param errMessage 错误信息
   * @return 错误响应对象
   */
  public static Response createBadResponse(Response.Status status,
      String errMessage) {
    Response response = Response.status(status)
        .entity(DOT_JOINER.join(status.toString(), errMessage)).build();
    return response;
  }

  /**
   * 判断应用是否处于运行状态
   * @param appState 应用状态
   * @return 是否运行中
   */
  public static boolean isRunningState(YarnApplicationState appState) {
    return appState == YarnApplicationState.RUNNING;
  }

  /**
   * 从HTTP请求中获取请求用户的UGI信息
   * @param req HTTP请求
   * @return 请求用户的UserGroupInformation对象
   */
  protected static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  /**
   * 从ResourceManager Web服务查询指定节点的NodeManager Web地址
   * @param yarnConf YARN配置
   * @param nodeId 节点ID
   * @return NodeManager Web地址
   * @throws JSONException JSON解析异常
   */
  public static String getNMWebAddressFromRM(Configuration yarnConf,
      String nodeId)
      throws JSONException {
    JSONObject nodeInfo = YarnWebServiceUtils.getNodeInfoFromRMWebService(yarnConf, nodeId)
        .getJSONObject("node");
    return nodeInfo.has("nodeHTTPAddress") ?
        nodeInfo.getString("nodeHTTPAddress") : null;
  }

  /**
   * 获取完整的NodeManager Web地址（自动补全http/https前缀）
   * @param yarnConf YARN配置
   * @param nmWebAddress 原始NodeManager Web地址
   * @return 带协议前缀的完整地址
   */
  public static String getAbsoluteNMWebAddress(Configuration yarnConf,
      String nmWebAddress) {
    if (nmWebAddress.contains(WebAppUtils.HTTP_PREFIX) || nmWebAddress
        .contains(WebAppUtils.HTTPS_PREFIX)) {
      return nmWebAddress;
    }
    return WebAppUtils.getHttpSchemePrefix(yarnConf) + nmWebAddress;
  }
}