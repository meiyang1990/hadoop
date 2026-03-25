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

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.common.Util;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.net.HttpURLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.io.*;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.security.SecurityUtil;
import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件系统镜像传输Servlet，运行在NameNode的Jetty服务器中，用于提供fsimage和edits文件的下载、上传服务
 * 
 * 核心功能：
 * 1. 非HA部署场景下，供SecondaryNameNode下载fsimage和edits文件用于定期检查点生成
 * 2. HA部署场景下，供Standby NameNode上传生成好的检查点镜像到Active NameNode，以及从Active NameNode拉取镜像做启动引导
 */
@InterfaceAudience.Private
public class ImageServlet extends HttpServlet {

  /** 该Servlet在Jetty中的访问路径 */
  public static final String PATH_SPEC = "/imagetransfer";

  private static final long serialVersionUID = -7669068179452648952L;

  private static final Logger LOG = LoggerFactory.getLogger(ImageServlet.class);

  public final static String CONTENT_DISPOSITION = "Content-Disposition";
  public final static String HADOOP_IMAGE_EDITS_HEADER = "X-Image-Edits-Name";
  
  private static final String TXID_PARAM = "txid";
  private static final String START_TXID_PARAM = "startTxId";
  private static final String END_TXID_PARAM = "endTxId";
  private static final String STORAGEINFO_PARAM = "storageInfo";
  private static final String LATEST_FSIMAGE_VALUE = "latest";
  private static final String IMAGE_FILE_TYPE = "imageFile";
  private static final String IS_BOOTSTRAP_STANDBY = "bootstrapstandby";

  /** 当前正在处理的检查点上传请求集合，线程安全，按事务ID排序 */
  private SortedSet<ImageUploadRequest> currentlyDownloadingCheckpoints = Collections
      .<ImageUploadRequest> synchronizedSortedSet(new TreeSet<ImageUploadRequest>());

  public static final String RECENT_IMAGE_CHECK_ENABLED =
      "recent.image.check.enabled";
  public static final boolean RECENT_IMAGE_CHECK_ENABLED_DEFAULT = true;

  /*
   * Specify a relaxation for the time delta check, the relaxation is to account
   * for the scenario that there are chances that minor time difference (e.g.
   * due to image upload delay, or minor machine clock skew) can cause ANN to
   * reject a fsImage too aggressively.
   */
  /** 最近镜像检查时间精度松弛系数，用于处理镜像上传延迟、时钟偏差导致的误拒绝 */
  private static double recentImageCheckTimePrecision = 0.75;

  @VisibleForTesting
  static void setRecentImageCheckTimePrecision(double ratio) {
    recentImageCheckTimePrecision = ratio;
  }

  /**
   * 从Servlet上下文获取并验证FSImage，检查NameNode是否初始化完成
   * @param context Servlet上下文
   * @param response HTTP响应对象，用于返回错误
   * @return 验证通过的FSImage对象
   * @throws IOException 如果NameNode未初始化完成
   */
  private FSImage getAndValidateFSImage(ServletContext context,
      final HttpServletResponse response)
      throws IOException {
    final FSImage nnImage = NameNodeHttpServer.getFsImageFromContext(context);
    if (nnImage == null) {
      String errorMsg = "NameNode initialization not yet complete. "
          + "FSImage has not been set in the NameNode.";
      sendError(response, HttpServletResponse.SC_FORBIDDEN, errorMsg);
      throw new IOException(errorMsg);
    }
    return nnImage;
  }

  @Override
  /**
   * 处理GET请求，用于下载fsimage、edits日志或别名映射表
   */
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    try {
      final ServletContext context = getServletContext();
      final FSImage nnImage = getAndValidateFSImage(context, response);
      final GetImageParams parsedParams = new GetImageParams(request, response);
      final Configuration conf = (Configuration) context
          .getAttribute(JspHelper.CURRENT_CONF);
      final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();

      validateRequest(context, conf, request, response, nnImage,
          parsedParams.getStorageInfoString());

      UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (parsedParams.isGetImage()) {
            // 处理fsimage下载请求
            long txid = parsedParams.getTxId();
            File imageFile = null;
            String errorMessage = "Could not find image";
            if (parsedParams.shouldFetchLatest()) {
              imageFile = nnImage.getStorage().getHighestFsImageName();
            } else {
              errorMessage += " with txid " + txid;
              imageFile = nnImage.getStorage().getFsImage(txid,
                  EnumSet.of(NameNodeFile.IMAGE, NameNodeFile.IMAGE_ROLLBACK));
            }
            if (imageFile == null) {
              throw new IOException(errorMessage);
            }
            CheckpointFaultInjector.getInstance().beforeGetImageSetsHeaders();
            long start = monotonicNow();
            serveFile(imageFile);

            // 记录下载耗时指标
            if (metrics != null) { // Metrics非空仅当在NameNode进程内运行
              long elapsed = monotonicNow() - start;
              metrics.addGetImage(elapsed);
            }
          } else if (parsedParams.isGetEdit()) {
            // 处理edits日志下载请求
            long startTxId = parsedParams.getStartTxId();
            long endTxId = parsedParams.getEndTxId();
            
            File editFile = nnImage.getStorage()
                .findFinalizedEditsFile(startTxId, endTxId);
            long start = monotonicNow();
            serveFile(editFile);

            // 记录下载耗时指标
            if (metrics != null) { // Metrics非空仅当在NameNode进程内运行
              long elapsed = monotonicNow() - start;
              metrics.addGetEdit(elapsed);
            }
          } else if (parsedParams.isGetAliasMap()) {
            // 处理别名映射表下载请求（用于联邦场景下的Standby引导）
            InMemoryAliasMap aliasMap =
                NameNodeHttpServer.getAliasMapFromContext(context);
            long start = monotonicNow();
            InMemoryAliasMap.transferForBootstrap(response, conf, aliasMap);
            // Metrics非空仅当在NameNode进程内运行
            if (metrics != null) {
              long elapsed = monotonicNow() - start;
              metrics.addGetAliasMap(elapsed);
            }
          }
          return null;
        }

        /**
         * 将指定文件写入HTTP响应流
         * @param file 要传输的文件
         * @throws IOException 如果传输失败
         */
        private void serveFile(File file) throws IOException {
          FileInputStream fis = new FileInputStream(file);
          try {
            // 设置MD5校验和、文件长度响应头
            setVerificationHeadersForGet(response, file);
            // 设置文件名响应头
            setFileNameHeaders(response, file);
            if (!file.exists()) {
              // 处理文件在设置头过程中被删除的竞态条件
              throw new FileNotFoundException(file.toString());
              // It's possible the file could be deleted after this point, but
              // we've already opened the 'fis' stream.
              // It's also possible length could change, but this would be
              // detected by the client side as an inaccurate length header.
            }
            // 根据是否是Standby引导选择对应的限速器
            DataTransferThrottler throttler = parsedParams.isBootstrapStandby ?
                getThrottlerForBootstrapStandby(conf) : getThrottler(conf);
            // 将文件内容拷贝到响应输出流
            TransferFsImage.copyFileToStream(response.getOutputStream(),
               file, fis, throttler);
          } finally {
            IOUtils.closeStream(fis);
          }
        }
      });
      
    } catch (Throwable t) {
      String errMsg = "GetImage failed. " + StringUtils.stringifyException(t);
      sendError(response, HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }

  /**
   * 验证镜像传输请求的合法性：检查请求者权限和存储信息匹配性
   * @param context Servlet上下文
   * @param conf Hadoop配置
   * @param request HTTP请求
   * @param response HTTP响应
   * @param nnImage NameNode的FSImage对象
   * @param theirStorageInfoString 请求方携带的存储信息字符串
   * @throws IOException 如果验证不通过
   */
  private void validateRequest(ServletContext context, Configuration conf,
      HttpServletRequest request, HttpServletResponse response,
      FSImage nnImage, String theirStorageInfoString) throws IOException {

    // 安全模式下验证请求者身份
    if (UserGroupInformation.isSecurityEnabled()
        && !isValidRequestor(context, request.getUserPrincipal().getName(),
            conf)) {
      String errorMsg = "Only Namenode, Secondary Namenode, and administrators may access "
          + "this servlet";
      sendError(response, HttpServletResponse.SC_FORBIDDEN, errorMsg);
      LOG.warn("Received non-NN/SNN/administrator request for image or edits from "
          + request.getUserPrincipal().getName()
          + " at "
          + request.getRemoteHost());
      throw new IOException(errorMsg);
    }

    // 验证存储信息匹配，确保请求方和当前NameNode属于同一集群
    String myStorageInfoString = nnImage.getStorage().toColonSeparatedString();
    if (theirStorageInfoString != null
        && !myStorageInfoString.equals(theirStorageInfoString)) {
      String errorMsg = "This namenode has storage info " + myStorageInfoString
          + " but the secondary expected " + theirStorageInfoString;
      sendError(response, HttpServletResponse.SC_FORBIDDEN, errorMsg);
      LOG.warn("Received an invalid request file transfer request "
          + "from a secondary with storage info " + theirStorageInfoString);
      throw new IOException(errorMsg);
    }
  }

  public static void setFileNameHeaders(HttpServletResponse response,
      File file) {
    response.setHeader(CONTENT_DISPOSITION, "attachment; filename=" +
        file.getName());
    response.setHeader(HADOOP_IMAGE_EDITS_HEADER, file.getName());
  }
  
  /**
   * 从配置构造普通镜像传输的限速器
   * @param conf 配置对象
   * @return 数据传输限速器，如果未配置带宽则返回null
   */
  public static DataTransferThrottler getThrottler(Configuration conf) {
    long transferBandwidth = conf.getLongBytes(
        DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
        DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }

  /**
   * 从配置构造Standby引导场景的镜像传输限速器
   * @param conf 配置对象
   * @return 数据传输限速器，如果未配置带宽则返回null
   */
  public static DataTransferThrottler getThrottlerForBootstrapStandby(
      Configuration conf) {
    long transferBandwidth =
        conf.getLongBytes(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }

  @VisibleForTesting
  /**
   * 验证请求者是否是合法的镜像传输请求来源（NameNode、SecondaryNameNode、其他Standby节点或管理员）
   * @param context Servlet上下文
   * @param remoteUser 请求者的Kerberos主体名
   * @param conf Hadoop配置
   * @return 如果请求合法返回true，否则返回false
   * @throws IOException 如果解析配置出错
   */
  static boolean isValidRequestor(ServletContext context, String remoteUser,
      Configuration conf) throws IOException {
    if (remoteUser == null) { // 正常情况不应该出现
      LOG.warn("Received null remoteUser while authorizing access to getImage servlet");
      return false;
    }

    Set<String> validRequestors = new HashSet<String>();

    // 添加当前NameNode自己的主体
    validRequestors.add(SecurityUtil.getServerPrincipal(conf
        .get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
        DFSUtilClient.getNNAddress(conf).getHostName()));
    try {
      // 添加SecondaryNameNode的主体
      validRequestors.add(
          SecurityUtil.getServerPrincipal(conf
              .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY),
              SecondaryNameNode.getHttpAddress(conf).getHostName()));
    } catch (Exception e) {
      // 解析SecondaryNameNode配置失败不阻塞，仅打日志警告
      LOG.debug("SecondaryNameNode principal could not be added", e);
      String msg = String.format(
        "SecondaryNameNode principal not considered, %s = %s, %s = %s",
        DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        conf.get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY),
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
          DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT));
      LOG.warn(msg);
    }

    // HA场景下添加其他Standby NameNode的主体
    if (HAUtil.isHAEnabled(conf, DFSUtil.getNamenodeNameServiceId(conf))) {
      List<Configuration> otherNnConfs = HAUtil.getConfForOtherNodes(conf);
      for (Configuration otherNnConf : otherNnConfs) {
        validRequestors.add(SecurityUtil.getServerPrincipal(otherNnConf
                .get(DFSConfigKeys.DFS_NAMENODE_K