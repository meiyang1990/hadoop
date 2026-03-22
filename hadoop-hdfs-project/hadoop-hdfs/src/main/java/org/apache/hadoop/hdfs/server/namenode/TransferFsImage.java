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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HttpPutFailedException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;
import org.apache.http.client.utils.URIBuilder;

import org.apache.hadoop.classification.VisibleForTesting;
import org.eclipse.jetty.io.EofException;

import static org.apache.hadoop.hdfs.server.common.Util.IO_FILE_BUFFER_SIZE;
import static org.apache.hadoop.hdfs.server.common.Util.connectionFactory;

/**
 * 文件级注释：提供从NameNode拉取和传输fsimage、edits等元数据文件的工具类，
 * 支持Standby节点启动引导、检查点同步等场景下的元数据传输。
 *
 * This class provides fetching a specified file from the NameNode.
 */
@InterfaceAudience.Private
public class TransferFsImage {

  /**
   * 枚举类型：元数据传输结果定义，封装HTTP响应码和是否需要抛出异常
   */
  public enum TransferResult{
    SUCCESS(HttpServletResponse.SC_OK, false),
    AUTHENTICATION_FAILURE(HttpServletResponse.SC_FORBIDDEN, true),
    NOT_ACTIVE_NAMENODE_FAILURE(HttpServletResponse.SC_EXPECTATION_FAILED, false),
    OLD_TRANSACTION_ID_FAILURE(HttpServletResponse.SC_CONFLICT, false),
    UNEXPECTED_FAILURE(-1, true);

    private final int response;
    private final boolean shouldReThrowException;

    TransferResult(int response, boolean rethrow) {
      this.response = response;
      this.shouldReThrowException = rethrow;
    }

    /**
     * 根据HTTP响应码匹配对应的传输结果枚举
     * @param code HTTP响应码
     * @return 匹配到的传输结果，未匹配到返回UNEXPECTED_FAILURE
     */
    public static TransferResult getResultForCode(int code){
      for(TransferResult result:TransferResult.values()){
        if(result.response == code){
          return result;
        }
      }
      return UNEXPECTED_FAILURE;
    }
  }

  @VisibleForTesting
  static int timeout = 0;
  private static final Logger LOG =
      LoggerFactory.getLogger(TransferFsImage.class);
  
  /**
   * 下载最新的fsimage镜像到指定目录
   * @param infoServer 源Active NameNode的HTTP地址
   * @param dir 目标存储目录
   * @throws IOException 下载过程中发生IO异常
   */
  public static void downloadMostRecentImageToDirectory(URL infoServer,
      File dir) throws IOException {
    String fileId = ImageServlet.getParamStringForMostRecentImage();
    getFileClient(infoServer, fileId, Lists.newArrayList(dir),
        null, false);
  }

  /**
   * 下载指定事务ID的fsimage镜像到NameNode存储目录
   * @param fsName 源Active NameNode的HTTP地址
   * @param imageTxId 目标fsimage的事务ID
   * @param dstStorage 目标NameNode存储对象
   * @param needDigest 是否需要返回文件MD5摘要
   * @param isBootstrapStandby 是否为Standby节点引导场景
   * @return 文件MD5摘要，needDigest为false时返回null
   * @throws IOException 下载过程中发生IO异常
   */
  public static MD5Hash downloadImageToStorage(URL fsName, long imageTxId,
      Storage dstStorage, boolean needDigest, boolean isBootstrapStandby)
      throws IOException {
    String fileid = ImageServlet.getParamStringForImage(null,
        imageTxId, dstStorage, isBootstrapStandby);
    String fileName = NNStorage.getCheckpointImageFileName(imageTxId);
    
    List<File> dstFiles = dstStorage.getFiles(
        NameNodeDirType.IMAGE, fileName);
    if (dstFiles.isEmpty()) {
      throw new IOException("No targets in destination storage!");
    }
    
    MD5Hash hash = getFileClient(fsName, fileid, dstFiles, dstStorage, needDigest);
    LOG.info("Downloaded file " + dstFiles.get(0).getName() + " size " +
        dstFiles.get(0).length() + " bytes.");
    return hash;
  }

  /**
   * 服务端处理上传fsimage请求，将上传的镜像写入存储
   * @param request HTTP请求对象
   * @param imageTxId 目标fsimage事务ID
   * @param dstStorage 目标存储对象
   * @param stream 请求输入流
   * @param advertisedSize 上传方声明的文件大小
   * @param throttler 传输限流工具
   * @return 接收文件的MD5摘要
   * @throws IOException 写入过程中发生IO异常
   */
  static MD5Hash handleUploadImageRequest(HttpServletRequest request,
      long imageTxId, Storage dstStorage, InputStream stream,
      long advertisedSize, DataTransferThrottler throttler) throws IOException {

    String fileName = NNStorage.getCheckpointImageFileName(imageTxId);

    List<File> dstFiles = dstStorage.getFiles(NameNodeDirType.IMAGE, fileName);
    if (dstFiles.isEmpty()) {
      throw new IOException("No targets in destination storage!");
    }

    MD5Hash advertisedDigest = parseMD5Header(request);
    MD5Hash hash = Util.receiveFile(fileName, dstFiles, dstStorage, true,
        advertisedSize, advertisedDigest, fileName, stream, throttler);
    LOG.info("Downloaded file " + dstFiles.get(0).getName() + " size "
        + dstFiles.get(0).length() + " bytes.");
    return hash;
  }

  /**
   * 下载指定范围的edits日志到NameNode存储目录
   * @param fsName 源Active NameNode的HTTP地址
   * @param log 远程edits日志信息，包含起始和结束事务ID
   * @param dstStorage 目标NameNode存储对象
   * @throws IOException 下载过程中发生IO异常
   */
  static void downloadEditsToStorage(URL fsName, RemoteEditLog log,
      NNStorage dstStorage) throws IOException {
    assert log.getStartTxId() > 0 && log.getEndTxId() > 0 :
      "bad log: " + log;
    String fileid = ImageServlet.getParamStringForLog(
        log, dstStorage);
    String finalFileName = NNStorage.getFinalizedEditsFileName(
        log.getStartTxId(), log.getEndTxId());

    List<File> finalFiles = dstStorage.getFiles(NameNodeDirType.EDITS,
        finalFileName);
    assert !finalFiles.isEmpty() : "No checkpoint targets.";
    
    // 如果文件已存在则跳过下载
    for (File f : finalFiles) {
      if (f.exists() && FileUtil.canRead(f)) {
        LOG.info("Skipping download of remote edit log " +
            log + " since it already is stored locally at " + f);
        return;
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Dest file: " + f);
      }
    }

    // 先下载到临时文件，完成后再重命名为最终文件名
    final long milliTime = Time.monotonicNow();
    String tmpFileName = NNStorage.getTemporaryEditsFileName(
        log.getStartTxId(), log.getEndTxId(), milliTime);
    List<File> tmpFiles = dstStorage.getFiles(NameNodeDirType.EDITS,
        tmpFileName);
    getFileClient(fsName, fileid, tmpFiles, dstStorage, false);
    LOG.info("Downloaded file " + tmpFiles.get(0).getName() + " size " +
        finalFiles.get(0).length() + " bytes.");

    // 注入故障点，用于测试
    CheckpointFaultInjector.getInstance().beforeEditsRename();

    // 将临时文件重命名为最终edits文件
    for (StorageDirectory sd : dstStorage.dirIterable(NameNodeDirType.EDITS)) {
      File tmpFile = NNStorage.getTemporaryEditsFile(sd,
          log.getStartTxId(), log.getEndTxId(), milliTime);
      File finalizedFile = NNStorage.getFinalizedEditsFile(sd,
          log.getStartTxId(), log.getEndTxId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Renaming " + tmpFile + " to " + finalizedFile);
      }
      boolean success = tmpFile.renameTo(finalizedFile);
      if (!success) {
        LOG.warn("Unable to rename edits file from " + tmpFile
            + " to " + finalizedFile);
      }
    }
  }

  /**
   * 从远程NameNode下载InMemoryAliasMap文件，用于视图联邦场景
   * @param fsName 远程NameNode的HTTP地址
   * @param aliasMap 目标本地文件路径
   * @param isBootstrapStandby 是否为Standby节点引导场景
   * @throws IOException 下载过程中发生IO异常
   */
  public static void downloadAliasMap(URL fsName, File aliasMap,
        boolean isBootstrapStandby) throws IOException {
    String paramString = ImageServlet.getParamStringForAliasMap(
        isBootstrapStandby);
    getFileClient(fsName, paramString, Arrays.asList(aliasMap), null, false);
    LOG.info("Downloaded file " + aliasMap.getName() + " size " +
        aliasMap.length() + " bytes.");
    InMemoryAliasMap.completeBootstrapTransfer(aliasMap);
  }

  /**
   * 从本地上传指定事务ID的fsimage到远程NameNode
   * @param fsName 远程NameNode的HTTP地址
   * @param conf Hadoop配置对象
   * @param storage 本地NameNode存储对象
   * @param nnf NameNode文件类型
   * @param txid 要上传的fsimage事务ID
   * @return 传输结果
   * @throws IOException 上传过程中发生IO异常
   */
  static TransferResult uploadImageFromStorage(URL fsName,
      Configuration conf, NNStorage storage, NameNodeFile nnf, long txid)
      throws IOException {
    return uploadImageFromStorage(fsName, conf, storage, nnf, txid, null);
  }

  /**
   * 从本地上传指定事务ID的fsimage到远程NameNode，支持取消上传
   * @param fsName 远程NameNode的HTTP地址
   * @param conf Hadoop配置对象
   * @param storage 本地NameNode存储对象
   * @param nnf NameNode文件类型
   * @param txid 要上传的fsimage事务ID
   * @param canceler 上传取消器，支持外部取消上传操作
   * @return 传输结果
   * @throws IOException 上传过程中发生IO异常或取消上传
   */
  public static TransferResult uploadImageFromStorage(URL fsName, Configuration conf,
      NNStorage storage, NameNodeFile nnf, long txid, Canceler canceler)
      throws IOException {
    URL url = new URL(fsName, ImageServlet.PATH_SPEC);
    long startTime = Time.monotonicNow();
    try {
      uploadImage(url, conf, storage, nnf, txId, canceler);
    } catch (HttpPutFailedException e) {
      // 将HTTP错误码转换为传输结果，根据结果决定是否抛出异常
      TransferResult result = TransferResult.getResultForCode(e.getResponseCode());
      if (result.shouldReThrowException) {
        throw e;
      }
      return result;
    }
    double xferSec = Math.max(
        ((float) (Time.monotonicNow() - startTime)) / 1000.0, 0.001);
    LOG.info("Uploaded image with txid " + txid + " to namenode at " + fsName
        + " in " + xferSec + " seconds");
    return TransferResult.SUCCESS;
  }

  /*
   * Uploads the imagefile using HTTP PUT method
   */
  /**
   * 使用HTTP PUT方法上传fsimage到远程NameNode
   * @param url 远程NameNode的请求URL
   * @param conf Hadoop配置对象
   * @param storage 本地NameNode存储对象
   * @param nnf NameNode文件类型
   * @param txId 要上传的fsimage事务ID
   * @param canceler 上传取消器
   * @throws IOException 上传过程中发生IO异常
   */
  private static void uploadImage(URL url, Configuration conf,
      NNStorage storage, NameNodeFile nnf, long txId, Canceler canceler)
      throws IOException {

    // 从本地存储查找对应事务ID的fsimage文件
    File imageFile = storage.findImageFile(nnf, txId);
    if (imageFile == null) {
      throw new IOException("Could not find image with txid " + txId);
    }

    HttpURLConnection connection = null;
    try {
      URIBuilder uriBuilder = new URIBuilder(url.toURI());

      // 将请求参数拼接到URL查询参数中，请求体存放fsimage内容
      Map<String, String> params = ImageServlet.getParamsForPutImage(storage,
          txId, imageFile.length(), nnf);
      for (Entry<String, String> entry : params.entrySet()) {
        uriBuilder.addParameter(entry.getKey(), entry.getValue());
      }

      // 打开HTTP连接，设置请求为PUT方法
      URL urlWithParams = uriBuilder.build().toURL();
      connection = (HttpURLConnection) connectionFactory.openConnection(
          urlWithParams, UserGroupInformation.isSecurityEnabled());
      // Set the request to PUT
      connection.setRequestMethod("PUT");
      connection.setDoOutput(true);

      
      // 获取分块传输大小配置
      int chunkSize = (int) conf.getLongBytes(
          DFSConfigKeys.DFS_IMAGE_TRANSFER_CHUNKSIZE_KEY,
          DFSConfigKeys.DFS_IMAGE_TRANSFER_CHUNKSIZE_DEFAULT);
      if (imageFile.length() > chunkSize) {
        // 大文件使用分块流式传输，支持上传超过2GB的文件，避免内部缓存
        connection.setChunkedStreamingMode(chunkSize);
      }

      // 设置连接和读取超时时间
      setTimeout(connection);

      // 设置MD5校验等验证请求头
      ImageServlet.setVerificationHeadersForPut(connection, imageFile);

      // 将文件写入HTTP请求输出流
      writeFileToPutRequest(conf, connection, imageFile, canceler, chunkSize);

      // 检查响应码，非200则抛出异常
      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new HttpPutFailedException(String.format(
            "Image uploading failed, status: %d, url: %s, message: %s",
            responseCode, urlWithParams, connection.getResponseMessage()),
            responseCode);
      }
    } catch (AuthenticationException | URISyntaxException e) {
      throw new IOException(e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  /**
   * 将本地fsimage文件写入HTTP PUT请求的输出流
   * @param conf Hadoop配置对象
   * @param connection HTTP连接对象
   * @param imageFile 本地要上传的fsimage文件
   * @param canceler 上传取消器
   * @param bufferSize 缓冲区大小
   * @throws IOException 写过程中发生IO异常
   */
  private static void writeFileToPutRequest(