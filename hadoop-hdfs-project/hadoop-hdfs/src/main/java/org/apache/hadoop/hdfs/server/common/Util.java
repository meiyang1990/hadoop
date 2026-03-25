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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;

import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS服务端通用工具类，提供URI处理、文件下载下载、地址解析、统计计算等通用工具能力，为HDFS核心服务提供公共支撑。
 * 该类为 final 类，不可被继承，所有方法均为静态工具方法。
 */
@InterfaceAudience.Private
public final class Util {
  private final static Logger LOG =
      LoggerFactory.getLogger(Util.class.getName());

  public final static String FILE_LENGTH = "File-Length";
  public final static String CONTENT_LENGTH = "Content-Length";
  public final static String MD5_HEADER = "X-MD5-Digest";
  public final static String CONTENT_TYPE = "Content-Type";
  public final static String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";

  public final static int IO_FILE_BUFFER_SIZE;
  private static final boolean isSpnegoEnabled;
  public static final URLConnectionFactory connectionFactory;

  static {
    // 加载默认配置初始化工具类
    Configuration conf = new Configuration();
    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);
    // 根据安全状态确定是否开启SPNEGO认证
    isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
    // 从配置中读取IO缓冲区大小
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }

  /**
   * 将字符串解析为URI，解析失败时默认按本地文件处理。
   * 用于处理配置中的路径字符串，兼容URI和本地文件两种格式。
   * @param s 待解析的字符串路径
   * @return 解析完成的URI对象
   */
  static URI stringAsURI(String s) throws IOException {
    URI u = null;
    // 尝试解析为标准URI
    try {
      u = new URI(s);
    } catch (URISyntaxException e){
      LOG.error("Syntax error in URI " + s
          + ". Please check hdfs configuration.", e);
    }

    // 如果解析失败或缺少scheme，默认按本地文件处理
    if(u == null || u.getScheme() == null){
      LOG.info("Assuming 'file' scheme for path " + s + " in configuration.");
      u = fileAsURI(new File(s));
    }
    return u;
  }

  /**
   * 将File对象转换为URI，自动去除目录路径末尾的多余斜杠。
   * @param f 待转换的File对象
   * @return 转换完成的标准URI
   */
  public static URI fileAsURI(File f) throws IOException {
    URI u = f.getCanonicalFile().toURI();
    
    // trim the trailing slash, if it's present
    if (u.getPath().endsWith("/")) {
      String uriAsString = u.toString();
      try {
        u = new URI(uriAsString.substring(0, uriAsString.length() - 1));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
    
    return u;
  }

  /**
   * 将字符串集合批量转换为URI集合。
   * @param names 待转换的字符串集合
   * @return 转换完成的URI集合
   */
  public static List<URI> stringCollectionAsURIs(
                                  Collection<String> names) {
    List<URI> uris = new ArrayList<>(names.size());
    for(String name : names) {
      try {
        uris.add(stringAsURI(name));
      } catch (IOException e) {
        LOG.error("Error while processing URI: " + name, e);
      }
    }
    return uris;
  }

  /**
   * 从指定URL下载文件到目标存储，支持校验和验证和流量控制。
   * 主要用于下载NameNode的fsimage镜像文件，在QJM共享存储和检查点场景使用。
   * @param url 下载目标URL
   * @param localPaths 本地存储路径列表
   * @param dstStorage 目标存储对象
   * @param getChecksum 是否需要验证MD5校验和
   * @param timeout HTTP连接超时时间
   * @param throttler 流量控速器
   * @return 计算得到的MD5校验值
   */
  public static MD5Hash doGetUrl(URL url, List<File> localPaths,
      Storage dstStorage, boolean getChecksum, int timeout,
      DataTransferThrottler throttler) throws IOException {
    HttpURLConnection connection;
    try {
      // 打开HTTP连接，处理认证
      connection = (HttpURLConnection)
          connectionFactory.openConnection(url, isSpnegoEnabled);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }

    // 设置连接和读取超时
    setTimeout(connection, timeout);

    // 检查响应状态是否正常
    if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new HttpGetFailedException("Image transfer servlet at " + url +
              " failed with status code " + connection.getResponseCode() +
              "\nResponse message:\n" + connection.getResponseMessage(),
          connection);
    }

    long advertisedSize;
    // 从响应头获取文件长度
    String contentLength = connection.getHeaderField(CONTENT_LENGTH);
    if (contentLength != null) {
      advertisedSize = Long.parseLong(contentLength);
    } else {
      throw new IOException(CONTENT_LENGTH + " header is not provided " +
          "by the namenode when trying to fetch " + url);
    }
    // 从响应头获取MD5校验值
    MD5Hash advertisedDigest = parseMD5Header(connection);
    // 从响应头获取文件名称
    String fsImageName = connection
        .getHeaderField(ImageServlet.HADOOP_IMAGE_EDITS_HEADER);
    // 获取输入流
    InputStream stream = connection.getInputStream();

    // 接收并保存文件
    return receiveFile(url.toExternalForm(), localPaths, dstStorage,
        getChecksum, advertisedSize, advertisedDigest, fsImageName, stream,
        throttler);
  }

  /**
   * 从输入流读取文件内容并保存到本地路径，支持校验和验证和流量控制。
   * @param url 原始下载URL，用于日志和错误信息
   * @param localPaths 本地存储路径列表
   * @param dstStorage 目标存储对象，用于错误报告
   * @param getChecksum 是否需要计算MD5校验和
   * @param advertisedSize 服务端声明的文件大小
   * @param advertisedDigest 服务端声明的MD5校验值
   * @param fsImageName 镜像文件名称
   * @param stream 输入流
   * @param throttler 流量控速器
   * @return 计算得到的MD5校验值
   */
  public static MD5Hash receiveFile(String url, List<File> localPaths,
      Storage dstStorage, boolean getChecksum, long advertisedSize,
      MD5Hash advertisedDigest, String fsImageName, InputStream stream,
      DataTransferThrottler throttler) throws
      IOException {
    // 记录开始时间用于统计传输速率
    long startTime = Time.monotonicNow();
    Map<FileOutputStream, File> streamPathMap = new HashMap<>();
    StringBuilder xferStats = new StringBuilder();
    double xferCombined = 0;
    if (localPaths != null) {
      // 如果本地路径是目录，使用服务端返回的文件名作为文件名
      List<File> newLocalPaths = new ArrayList<>();
      for (File localPath : localPaths) {
        if (localPath.isDirectory()) {
          if (fsImageName == null) {
            throw new IOException("No filename header provided by server");
          }
          newLocalPaths.add(new File(localPath, fsImageName));
        } else {
          newLocalPaths.add(localPath);
        }
      }
      localPaths = newLocalPaths;
    }


    long received = 0;
    MessageDigest digester = null;
    if (getChecksum) {
      // 如果需要校验，初始化MD5计算器并包装输入流
      digester = MD5Hash.getDigester();
      stream = new DigestInputStream(stream, digester);
    }
    boolean finishedReceiving = false;
    int num = 1;

    List<FileOutputStream> outputStreams = Lists.newArrayList();

    try {
      if (localPaths != null) {
        // 为每个路径打开输出流
        for (File f : localPaths) {
          try {
            if (f.exists()) {
              LOG.warn("Overwriting existing file " + f
                  + " with file downloaded from " + url);
            }
            FileOutputStream fos = new FileOutputStream(f);
            outputStreams.add(fos);
            streamPathMap.put(fos, f);
          } catch (IOException ioe) {
            LOG.warn("Unable to download file " + f, ioe);
            // 如果打开文件失败，向存储报告该磁盘错误
            if (dstStorage != null &&
                (dstStorage instanceof StorageErrorReporter)) {
              ((StorageErrorReporter)dstStorage).reportErrorOnFile(f);
            }
          }
        }

        if (outputStreams.isEmpty()) {
          throw new IOException(
              "Unable to download to any storage directory");
        }
      }

      byte[] buf = new byte[IO_FILE_BUFFER_SIZE];
      while (num > 0) {
        // 循环读取数据
        num = stream.read(buf);
        if (num > 0) {
          received += num;
          // 写入所有输出流
          for (FileOutputStream fos : outputStreams) {
            fos.write(buf, 0, num);
          }
          // 流量控制
          if (throttler != null) {
            throttler.throttle(num);
          }
        }
      }
      finishedReceiving = true;
      // 计算下载耗时和速率
      double xferSec = Math.max(
          ((float)(Time.monotonicNow() - startTime)) / 1000.0, 0.001);
      long xferKb = received / 1024;
      xferCombined += xferSec;
      xferStats.append(
          String.format(" The file download took %.2fs at %.2f KB/s.",
              xferSec, xferKb / xferSec));
    } finally {
      // 关闭输入流
      stream.close();
      // 刷盘并关闭所有输出流
      for (FileOutputStream fos : outputStreams) {
        long flushStartTime = Time.monotonicNow();
        fos.getChannel().force(true);
        fos.close();
        // 统计刷盘耗时
        double writeSec = Math.max(((float)
            (Time.monotonicNow() - flushStartTime)) / 1000.0, 0.001);
        xferCombined += writeSec;
        xferStats.append(String
            .format(" Synchronous (fsync) write to disk of " +
                streamPathMap.get(fos).getAbsolutePath() +
                " took %.2fs.", writeSec));
      }

      // 如果接收未完成，删除所有临时文件
      if (!finishedReceiving) {
        deleteTmpFiles(localPaths);
      }

      // 接收完成但文件大小不匹配，删除文件并抛出异常
      if (finishedReceiving && received != advertisedSize) {
        deleteTmpFiles(localPaths);
        throw new IOException("File " + url + " received length " + received +
            " is not of the advertised size " + advertisedSize +
            ". Fsimage name: " + fsImageName + " lastReceived: " + num);
      }
    }
    // 输出整体传输统计信息
    xferStats.insert(0, String.format("Combined time for file download and" +
        " fsync to all disks took %.2fs.", xferCombined));
    LOG.info(xferStats.toString());

    if (digester != null) {
      // 计算MD5并和服务端声明对比
      MD5Hash computedDigest = new MD5Hash(digester.digest());

      if (advertisedDigest != null &&
          !computedDigest.equals(advertisedDigest)) {
        deleteTmpFiles(localPaths);
        throw new IOException("File " + url + " computed digest " +
            computedDigest + " does not match advertised digest " +
            advertisedDigest);
      }
      return computedDigest;
    } else {
      return null;
    }
  }

  /**
   * 删除下载过程中产生的临时文件。
   * @param files 待删除的文件列表
   */
  private static void deleteTmpFiles(List<File> files) {
    if (files == null) {
      return;
    }

    LOG.info("Deleting temporary files: " + files);
    for (File file : files) {
      if (!file.delete()) {
        LOG.warn("Deleting " + file + " has failed");
      }
    }
  }

  /**
   * 为HTTP连接设置连接超时和读取超时。
   * @param connection HTTP连接对象
   * @param timeout 超时时间，单位毫秒，小于等于0不设置
   */
  public static void setTimeout(HttpURLConnection connection, int timeout) {
    if (timeout > 0) {
      connection.setConnectTimeout(timeout);
      connection.setReadTimeout(timeout);
    }
  }

  /**
   * 从HTTP响应头解析MD5校验值。
   * @param connection HTTP连接对象
   * @return 解析得到的MD5，响应头不存在返回null
   */
  private static MD5Hash parseMD5Header(HttpURLConnection connection) {
    String header = connection.getHeaderField(MD5_HEADER);
    return (header != null) ? new MD5Hash(header) : null;
  }

  /**
   * 从URI中解析地址列表，支持域名解析多个IP，用于QJM共享存储的JournalNode地址解析。
   * @param uri 包含多个地址的URI，地址之间用分号分隔
   * @param conf Hadoop配置对象
   * @return 解析完成的InetSocketAddress列表
   */
  public static List<InetSocketAddress> getAddressesList(URI uri, Configuration conf)
      throws IOException{
    String authority = uri.getAuthority();
    Preconditions.checkArgument(authority != null && !authority.isEmpty(),
        "URI has no authority: " + uri);

    // 按分号分隔多个地址
    String[] parts = StringUtils.split(authority, ';');
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim();
    }

    // 检查是否开启域名解析
    boolean resolveNeeded = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_EDITS_QJOURNALS_RESOLUTION_ENABLED,
        DFSConfigKeys.DFS_NAMENODE_EDITS_QJOURNALS_RESOLUTION_ENABLED_DEFAULT);
    // 创建域名解析器
    DomainNameResolver dnr = DomainNameResolverFactory.newInstance(
        conf,
        DFSConfigKeys.DFS_NAMENODE_EDITS_QJOURNALS_RESOLUTION_RESOLVER_IMPL);

    List<InetSocketAddress> addrs = Lists.newArrayList();
    for (String addr : parts) {
      if (resolveNeeded) {