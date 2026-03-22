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
package org.apache.hadoop.hdfs.server.namenode.web.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * WebHDFS REST API 在NameNode端的实现类
 * 负责处理所有对NameNode的WebHDFS请求，包括元数据操作、文件/目录增删改查、重定向数据读写请求到对应DataNode
 */
@Path("")
public class NamenodeWebHdfsMethods {
  public static final Logger LOG =
      LoggerFactory.getLogger(NamenodeWebHdfsMethods.class);

  private static final UriFsPathParam ROOT = new UriFsPathParam("");

  private volatile Boolean useIpcCallq;
  private String scheme;
  private Principal userPrincipal;
  private String remoteAddr;
  private int remotePort;

  private @Context ServletContext context;
  private @Context HttpServletResponse response;
  private boolean supportEZ;

  /**
   * 构造函数，从HttpServletRequest提取请求信息，用于后续外部线程处理
   * @param request HTTP请求对象
   */
  public NamenodeWebHdfsMethods(@Context HttpServletRequest request) {
    // the request object is a proxy to thread-locals so we have to extract
    // what we want from it since the external call will be processed in a
    // different thread.
    scheme = request.getScheme();
    userPrincipal = request.getUserPrincipal();
    // get the remote address, if coming in via a trusted proxy server then
    // the address with be that of the proxied client
    remoteAddr = JspHelper.getRemoteAddr(request);
    remotePort = JspHelper.getRemotePort(request);
    supportEZ =
        Boolean.valueOf(request.getHeader(WebHdfsFileSystem.EZ_HEADER));
  }

  /**
   * 初始化WebHDFS请求，读取配置、记录日志、重置响应内容类型
   * @param ugi 请求用户的用户组信息
   * @param delegation 委派令牌参数
   * @param username 用户名参数
   * @param doAsUser 代理用户参数
   * @param path 请求路径参数
   * @param op HTTP操作参数
   * @param parameters 其他参数列表
   */
  protected void init(final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final UriFsPathParam path, final HttpOpParam<?> op,
      final Param<?, ?>... parameters) {
    if (useIpcCallq == null) {
      Configuration conf =
          (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
      useIpcCallq = conf.getBoolean(
          DFSConfigKeys.DFS_WEBHDFS_USE_IPC_CALLQ,
          DFSConfigKeys.DFS_WEBHDFS_USE_IPC_CALLQ_DEFAULT);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP " + op.getValue().getType() + ": " + op + ", " + path
          + ", ugi=" + ugi + ", " + username + ", " + doAsUser
          + Param.toSortedString(", ", parameters));
    }

    //clear content type
    response.setContentType(null);
  }

  private static NamenodeProtocols getRPCServer(NameNode namenode)
      throws IOException {
     final NamenodeProtocols np = namenode.getRpcServer();
     if (np == null) {
       throw new RetriableException("Namenode is in startup mode");
     }
     return np;
  }

  /**
   * 获取NameNode的RPC客户端协议对象，检查NameNode是否已启动完成
   * @return ClientProtocol RPC协议对象
   * @throws IOException 如果NameNode还未初始化完成抛出异常
   */
  protected ClientProtocol getRpcClientProtocol() throws IOException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final ClientProtocol cp = namenode.getRpcServer();
    if (cp == null) {
      throw new RetriableException("Namenode is in startup mode");
    }
    return cp;
  }

  protected String getScheme() {
    return scheme;
  }

  protected ServletContext getContext() {
    return context;
  }

  /**
   * 在指定用户身份下执行操作，根据配置选择直接执行或放入IPC调用队列执行
   * @param ugi 目标用户身份
   * @param action 要执行的特权操作
   * @return 操作结果
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  private <T> T doAs(final UserGroupInformation ugi,
      final PrivilegedExceptionAction<T> action)
          throws IOException, InterruptedException {
    return useIpcCallq ? doAsExternalCall(ugi, action) : ugi.doAs(action);
  }

  /**
   * 将操作封装为外部调用放入NameNode的调用队列执行，用于统一限流和排队
   * @param ugi 目标用户身份
   * @param action 要执行的特权操作
   * @return 操作结果
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  private <T> T doAsExternalCall(final UserGroupInformation ugi,
      final PrivilegedExceptionAction<T> action)
          throws IOException, InterruptedException {
    // set the remote address, if coming in via a trust proxy server then
    // the address with be that of the proxied client
    ExternalCall<T> call = new ExternalCall<T>(action){
      @Override
      public UserGroupInformation getRemoteUser() {
        return ugi;
      }
      @Override
      public String getProtocol() {
        return "webhdfs";
      }
      @Override
      public String getHostAddress() {
        return getRemoteAddr();
      }
      @Override
      public int getRemotePort() {
        return getRemotePortFromJSPHelper();
      }
      @Override
      public InetAddress getHostInetAddress() {
        try {
          return InetAddress.getByName(getHostAddress());
        } catch (UnknownHostException e) {
          return null;
        }
      }
    };

    queueExternalCall(call);
    T result = null;
    try {
      result = call.get();
    } catch (ExecutionException ee) {
      Throwable t = ee.getCause();
      if (t instanceof RuntimeException) {
        throw (RuntimeException)t;
      } else if (t instanceof IOException) {
        throw (IOException)t;
      } else {
        throw new IOException(t);
      }
    }
    return result;
  }

  protected String getRemoteAddr() {
    return remoteAddr;
  }

  protected int getRemotePortFromJSPHelper() {
    return remotePort;
  }

  /**
   * 将外部调用放入NameNode的调用队列
   * @param call 要排队的外部调用对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  protected void queueExternalCall(ExternalCall call)
      throws IOException, InterruptedException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    namenode.queueExternalCall(call);
  }

  /**
   * 为WebHDFS请求选择合适的DataNode进行重定向
   * 根据操作类型和块位置选择包含目标块、排除已下线和指定排除节点的最优DataNode
   * @param namenode NameNode实例
   * @param path 请求的HDFS路径
   * @param op HTTP操作类型
   * @param openOffset 打开文件的偏移量
   * @param blocksize 块大小
   * @param excludeDatanodes 需要排除的DataNode列表
   * @param remoteAddr 客户端地址
   * @param status 文件状态信息
   * @return 选中的DataNode信息
   * @throws IOException 如果找不到可用DataNode或文件不存在抛出异常
   */
  @VisibleForTesting
  static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, final String excludeDatanodes,
      final String remoteAddr, final HdfsFileStatus status) throws IOException {
    FSNamesystem fsn = namenode.getNamesystem();
    if (fsn == null) {
      throw new IOException("Namesystem has not been initialized yet.");
    }
    final BlockManager bm = fsn.getBlockManager();

    Set<Node> excludes = new HashSet<>();
    if (excludeDatanodes != null) {
      for (String host : StringUtils
          .getTrimmedStringCollection(excludeDatanodes)) {
        int idx = host.indexOf(':');
        Node excludeNode = null;
        if (idx == -1) {
          excludeNode = bm.getDatanodeManager().getDatanodeByHost(host);
        } else {
          excludeNode = bm.getDatanodeManager().getDatanodeByXferAddr(
            host.substring(0, idx), Integer.parseInt(host.substring(idx + 1)));
        }

        if (excludeNode != null) {
          excludes.add(excludeNode);
        } else {
          LOG.debug("DataNode {} was requested to be excluded, "
                + "but it was not found.", host);
        }
      }
    }

    // For these operations choose a datanode containing a replica
    if (op == GetOpParam.Op.OPEN
        || op == GetOpParam.Op.GETFILECHECKSUM
        || op == PostOpParam.Op.APPEND) {
      final NamenodeProtocols np = getRPCServer(namenode);
      if (status == null) {
        throw new FileNotFoundException("File " + path + " not found.");
      }

      final long len = status.getLen();
      if (op == GetOpParam.Op.OPEN) {
        if (openOffset < 0L || (openOffset >= len && len > 0)) {
          throw new IOException("Offset=" + openOffset
              + " out of the range [0, " + len + "); " + op + ", path=" + path);
        }
      }

      if (len > 0) {
        final long offset = op == GetOpParam.Op.OPEN? openOffset: len - 1;
        final LocatedBlocks locations = np.getBlockLocations(path, offset, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          return bestNode(locations.get(0).getLocations(), excludes);
        } else {
          throw new IOException("Block could not be located. Path=" + path + ", offset=" + offset);
        }
      }
    }

    // All other operations don't affect a specific node so let the BlockManager pick a target
    DatanodeDescriptor clientNode = bm.getDatanodeManager(
    ).getDatanodeByHost(remoteAddr);

    DatanodeStorageInfo[] storages = bm.chooseTarget4WebHDFS(
      path, clientNode, excludes, blocksize);
    if (storages.length > 0) {
      return storages[0].getDatanodeDescriptor();
    }

    return (DatanodeDescriptor)bm.getDatanodeManager().getNetworkTopology(
        ).chooseRandom(NodeBase.ROOT, excludes);
  }

  /**
   * 从候选DataNode列表中选择第一个可用且未被排除的节点
   * 输入列表已经按可用性和网络距离排序，直接返回第一个符合条件的节点即可
   * @param nodes 候选DataNode数组，已排序
   * @param excludes 需要排除的节点集合
   * @return 第一个符合条件的DataNode
   * @throws IOException 如果没有符合条件的节点抛出异常
   */
  protected static DatanodeInfo bestNode(DatanodeInfo[] nodes,
      Set<Node> excludes) throws IOException {
    for (DatanodeInfo dn: nodes) {
      if (!dn.isDecommissioned() && !excludes.contains(dn)) {
        return dn;
      }
    }
    throw new IOException("No active and not excluded nodes contain this block");
  }

  /**
   * 续期委派令牌