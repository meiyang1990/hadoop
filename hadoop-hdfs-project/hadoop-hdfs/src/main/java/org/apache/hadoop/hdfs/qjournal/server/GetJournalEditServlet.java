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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.hdfs.server.namenode.DfsServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

/**
 * QJM共享编辑日志的HTTP获取服务Servlet，用于以下两种场景：
 * <ul>
 * <li>QuorumJournalManager读取编辑日志时，从JournalNode拉取编辑流</li>
 * <li>编辑日志同步阶段，一个JournalNode从另一个JournalNode拉取编辑日志</li>
 * </ul>
 * 该类是HDFS QJM（Quorum Journal Manager）架构中JournalNode对外提供编辑日志读取的HTTP服务端点，
 * 用于支持NameNode读取 edits 和Journal节点间的数据同步。
 */
@InterfaceAudience.Private
public class GetJournalEditServlet extends DfsServlet {

  private static final long serialVersionUID = -4635891628211723009L;
  private static final Logger LOG =
      LoggerFactory.getLogger(GetJournalEditServlet.class);

  static final String STORAGEINFO_PARAM = "storageInfo";
  static final String JOURNAL_ID_PARAM = "jid";
  static final String SEGMENT_TXID_PARAM = "segmentTxId";
  static final String IN_PROGRESS_OK = "inProgressOk";

  /**
   * 验证请求发起者是否为合法的请求主体（NameNode或其他JournalNode）。
   * @param request HTTP请求对象
   * @param conf Hadoop配置对象
   * @return 是否为合法请求
   * @throws IOException 解析用户信息时抛出IO异常
   */
  protected boolean isValidRequestor(HttpServletRequest request, Configuration conf)
      throws IOException {
    UserGroupInformation ugi = getUGI(request, conf);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Validating request made by " + ugi.getUserName() +
          " / " + ugi.getShortUserName() + ". This user is: " +
          UserGroupInformation.getLoginUser());
    }

    Set<String> validRequestors = new HashSet<String>();
    // 添加所有配置中的NameNode主体到合法请求列表
    validRequestors.addAll(DFSUtil.getAllNnPrincipals(conf));
    try {
      // 添加SecondaryNameNode主体到合法请求列表
      validRequestors.add(
          SecurityUtil.getServerPrincipal(conf
              .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY),
              SecondaryNameNode.getHttpAddress(conf).getHostName()));
    } catch (Exception e) {
      // 添加失败不中断流程，仅记录日志
      LOG.debug("SecondaryNameNode principal could not be added", e);
      String msg = String.format(
        "SecondaryNameNode principal not considered, %s = %s, %s = %s",
        DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        conf.get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY),
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        conf.get(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
          DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT));
      LOG.warn(msg);
    }

    // 遍历所有已配置合法主体，匹配完整主体名
    for (String v : validRequestors) {
      if (LOG.isDebugEnabled())
        LOG.debug("isValidRequestor is comparing to valid requestor: " + v);
      if (v != null && v.equals(ugi.getUserName())) {
        if (LOG.isDebugEnabled())
          LOG.debug("isValidRequestor is allowing: " + ugi.getUserName());
        return true;
      }
    }

    // 额外允许其他JournalNode请求：比较短用户名，因为恢复阶段无法预先枚举所有JournalNode
    if (ugi.getShortUserName().equals(
          UserGroupInformation.getLoginUser().getShortUserName())) {
      if (LOG.isDebugEnabled())
        LOG.debug("isValidRequestor is allowing other JN principal: " +
            ugi.getUserName());
      return true;
    }

    if (LOG.isDebugEnabled())
      LOG.debug("isValidRequestor is rejecting: " + ugi.getUserName());
    return false;
  }
  
  /**
   * 检查请求发起者合法性，不合法则返回错误响应。
   * @param conf Hadoop配置对象
   * @param request HTTP请求对象
   * @param response HTTP响应对象
   * @return 合法返回true，不合法返回false并已写入错误响应
   * @throws IOException 发送错误响应时抛出IO异常
   */
  private boolean checkRequestorOrSendError(Configuration conf,
      HttpServletRequest request, HttpServletResponse response)
          throws IOException {
    if (UserGroupInformation.isSecurityEnabled()
        && !isValidRequestor(request, conf)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
          "Only Namenode and another JournalNode may access this servlet");
      LOG.warn("Received non-NN/JN request for edits from "
          + request.getRemoteHost());
      return false;
    }
    return true;
  }
  
  /**
   * 检查请求携带的命名空间信息是否与当前JournalNode存储匹配，不匹配则返回错误响应。
   * @param storage 当前JournalNode存储对象
   * @param request HTTP请求对象
   * @param response HTTP响应对象
   * @return 匹配返回true，不匹配返回false并已写入错误响应
   * @throws IOException 发送错误响应时抛出IO异常
   */
  private boolean checkStorageInfoOrSendError(JNStorage storage,
      HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    int myNsId = storage.getNamespaceID();
    String myClusterId = storage.getClusterID();
    
    String theirStorageInfoString = StringEscapeUtils.escapeHtml4(
        request.getParameter(STORAGEINFO_PARAM));

    if (theirStorageInfoString != null) {
      // 解析请求携带的命名空间ID和集群ID
      int theirNsId = StorageInfo.getNsIdFromColonSeparatedString(
          theirStorageInfoString);
      String theirClusterId = StorageInfo.getClusterIdFromColonSeparatedString(
          theirStorageInfoString);
      // 匹配命名空间和集群ID，不匹配则拒绝请求
      if (myNsId != theirNsId || !myClusterId.equals(theirClusterId)) {
        String msg = "This node has namespaceId '" + myNsId + " and clusterId '"
            + myClusterId + "' but the requesting node expected '" + theirNsId
            + "' and '" + theirClusterId + "'";
        response.sendError(HttpServletResponse.SC_FORBIDDEN, msg);
        LOG.warn("Received an invalid request file transfer request from " +
            request.getRemoteAddr() + ": " + msg);
        return false;
      }
    }
    return true;
  }
  
  /**
   * 处理HTTP GET请求，返回指定起始事务ID的编辑日志文件内容。
   * @param request HTTP请求对象
   * @param response HTTP响应对象
   * @throws ServletException Servlet处理异常
   * @throws IOException IO处理异常
   */
  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    FileInputStream editFileIn = null;
    try {
      final ServletContext context = getServletContext();
      // 从Servlet上下文获取Hadoop配置
      final Configuration conf = (Configuration) getServletContext()
          .getAttribute(JspHelper.CURRENT_CONF);
      // 获取请求参数：日志ID
      final String journalId = request.getParameter(JOURNAL_ID_PARAM);
      // 获取请求参数：是否允许返回进行中的分段
      final String inProgressOkStr = request.getParameter(IN_PROGRESS_OK);
      final boolean inProgressOk;
      if (inProgressOkStr != null &&
          inProgressOkStr.equalsIgnoreCase("false")) {
        inProgressOk = false;
      } else {
        inProgressOk = true;
      }
      // 校验日志ID格式合法性
      QuorumJournalManager.checkJournalId(journalId);
      // 从上下文获取对应日志ID的存储对象
      final JNStorage storage = JournalNodeHttpServer
          .getJournalFromContext(context, journalId).getStorage();

      // 检查请求发起者合法性
      if (!checkRequestorOrSendError(conf, request, response)) {
        return;
      }

      // 检查命名空间信息一致性
      if (!checkStorageInfoOrSendError(storage, request, response)) {
        return;
      }
      
      // 解析请求参数：分段起始事务ID
      long segmentTxId = ServletUtil.parseLongParam(request,
          SEGMENT_TXID_PARAM);

      FileJournalManager fjm = storage.getJournalManager();
      File editFile;

      // 加锁防止文件在打开过程中被finalize修改
      synchronized (fjm) {
        // 获取对应事务ID的编辑日志文件
        EditLogFile elf = fjm.getLogFile(segmentTxId, inProgressOk);
        if (elf == null) {
          response.sendError(HttpServletResponse.SC_NOT_FOUND,
              "No edit log found starting at txid " + segmentTxId);
          return;
        }
        editFile = elf.getFile();
        // 设置文件校验响应头
        ImageServlet.setVerificationHeadersForGet(response, editFile);
        // 设置文件名响应头
        ImageServlet.setFileNameHeaders(response, editFile);
        // 打开文件输入流
        editFileIn = new FileInputStream(editFile);
      }
      
      // 获取带宽限流控制器
      DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

      // 将编辑日志文件拷贝到响应输出流返回给请求方
      TransferFsImage.copyFileToStream(response.getOutputStream(), editFile,
          editFileIn, throttler);

    } catch (Throwable t) {
      String errMsg = "getedit failed. " + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errMsg);
      throw new IOException(errMsg);
    } finally {
      IOUtils.closeStream(editFileIn);
    }
  }

  /**
   * 构建获取编辑日志的请求路径，携带所有必要请求参数。
   * @param journalId 日志ID
   * @param segmentTxId 分段起始事务ID
   * @param nsInfo 命名空间信息
   * @param inProgressOk 是否允许返回进行中的分段
   * @return 编码完成的请求路径
   */
  public static String buildPath(String journalId, long segmentTxId,
      NamespaceInfo nsInfo, boolean inProgressOk) {
    StringBuilder path = new StringBuilder("/getJournal?");
    try {
      path.append(JOURNAL_ID_PARAM).append("=")
          .append(URLEncoder.encode(journalId, "UTF-8"));
      path.append("&" + SEGMENT_TXID_PARAM).append("=")
          .append(segmentTxId);
      path.append("&" + STORAGEINFO_PARAM).append("=")
          .append(URLEncoder.encode(nsInfo.toColonSeparatedString(), "UTF-8"));
      path.append("&" + IN_PROGRESS_OK).append("=")
          .append(inProgressOk);
    } catch (UnsupportedEncodingException e) {
      // UTF-8必然支持，不会走到这里
      throw new RuntimeException(e);
    }
    return path.toString();
  }
}