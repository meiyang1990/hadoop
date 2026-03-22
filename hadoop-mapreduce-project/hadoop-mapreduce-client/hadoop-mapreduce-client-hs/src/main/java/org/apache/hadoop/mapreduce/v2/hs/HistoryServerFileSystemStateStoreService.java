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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：基于Hadoop FileSystem接口实现的MapReduce历史服务器状态存储服务
 * 核心功能：将MR代理令牌和令牌主密钥持久化存储在兼容FileSystem接口的存储系统中，
 * 支持历史服务器重启后恢复认证状态，保证代理令牌的持续有效性
 */
@Private
@Unstable
/**
 * A history server state storage implementation that supports any persistent
 * storage that adheres to the FileSystem interface.
 */
public class HistoryServerFileSystemStateStoreService
    extends HistoryServerStateStoreService {

  public static final Logger LOG =
      LoggerFactory.getLogger(HistoryServerFileSystemStateStoreService.class);

  private static final String ROOT_STATE_DIR_NAME = "HistoryServerState";
  private static final String TOKEN_STATE_DIR_NAME = "tokens";
  private static final String TOKEN_KEYS_DIR_NAME = "keys";
  private static final String TOKEN_BUCKET_DIR_PREFIX = "tb_";
  private static final String TOKEN_BUCKET_NAME_FORMAT =
      TOKEN_BUCKET_DIR_PREFIX + "%03d";
  private static final String TOKEN_MASTER_KEY_FILE_PREFIX = "key_";
  private static final String TOKEN_FILE_PREFIX = "token_";
  private static final String TMP_FILE_PREFIX = "tmp-";
  private static final String UPDATE_TMP_FILE_PREFIX = "update-";
  private static final FsPermission DIR_PERMISSIONS =
      new FsPermission((short)0700);
  private static final FsPermission FILE_PERMISSIONS = Shell.WINDOWS
      ? new FsPermission((short) 0700) : new FsPermission((short) 0400);
  private static final int NUM_TOKEN_BUCKETS = 1000;

  private FileSystem fs;
  private Path rootStatePath;
  private Path tokenStatePath;
  private Path tokenKeysStatePath;

  /**
   * 初始化存储，从配置中读取存储路径并设置根目录
   * @param conf 历史服务器配置对象
   * @throws IOException 当未配置存储URI时抛出异常
   */
  @Override
  protected void initStorage(Configuration conf)
      throws IOException {
    final String storeUri = conf.get(JHAdminConfig.MR_HS_FS_STATE_STORE_URI);
    if (storeUri == null) {
      throw new IOException("No store location URI configured in " +
          JHAdminConfig.MR_HS_FS_STATE_STORE_URI);
    }

    LOG.info("Using " + storeUri + " for history server state storage");
    rootStatePath = new Path(storeUri, ROOT_STATE_DIR_NAME);
  }

  /**
   * 启动存储服务，创建目录结构和分桶目录
   * @throws IOException 创建目录失败时抛出异常
   */
  @Override
  protected void startStorage() throws IOException {
    fs = createFileSystem();
    createDir(rootStatePath);
    tokenStatePath = new Path(rootStatePath, TOKEN_STATE_DIR_NAME);
    createDir(tokenStatePath);
    tokenKeysStatePath = new Path(tokenStatePath, TOKEN_KEYS_DIR_NAME);
    createDir(tokenKeysStatePath);
    // 预先创建所有令牌分桶目录，分散令牌存储提升读写性能
    for (int i=0; i < NUM_TOKEN_BUCKETS; ++i) {
      createDir(getTokenBucketPath(i));
    }
  }

  /**
   * 创建存储状态使用的FileSystem实例
   * @return 对应存储路径的FileSystem对象
   * @throws IOException 创建FileSystem失败时抛出异常
   */
  FileSystem createFileSystem() throws IOException {
    return rootStatePath.getFileSystem(getConfig());
  }

  @Override
  protected void closeStorage() throws IOException {
    // don't close the filesystem as it's part of the filesystem cache
    // and other clients may still be using it
  }

  /**
   * 从文件系统加载历史服务器完整状态，包括所有代理令牌和主密钥
   * @return 加载完成的历史服务器状态对象
   * @throws IOException 加载状态失败时抛出异常
   */
  @Override
  public HistoryServerState loadState() throws IOException {
    LOG.info("Loading history server state from " + rootStatePath);
    HistoryServerState state = new HistoryServerState();
    loadTokenState(state);
    return state;
  }

  /**
   * 持久化存储新增的MR代理令牌
   * @param tokenId 代理令牌标识符
   * @param renewDate 令牌更新时间
   * @throws IOException 存储失败或令牌已存在时抛出异常
   */
  @Override
  public void storeToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token " + tokenId.getSequenceNumber());
    }

    Path tokenPath = getTokenPath(tokenId);
    if (fs.exists(tokenPath)) {
      throw new IOException(tokenPath + " already exists");
    }

    createNewFile(tokenPath, buildTokenData(tokenId, renewDate));
  }

  /**
   * 更新已有代理令牌的更新时间
   * 通过临时文件重命名保证更新原子性，避免更新过程中服务异常导致数据损坏
   * @param tokenId 代理令牌标识符
   * @param renewDate 新的更新时间
   * @throws IOException 更新失败时抛出异常
   */
  @Override
  public void updateToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating token " + tokenId.getSequenceNumber());
    }

    // Files cannot be atomically replaced, therefore we write a temporary
    // update file, remove the original token file, then rename the update
    // file to the token file. During recovery either the token file will be
    // used or if that is missing and an update file is present then the
    // update file is used.
    Path tokenPath = getTokenPath(tokenId);
    Path tmp = new Path(tokenPath.getParent(),
        UPDATE_TMP_FILE_PREFIX + tokenPath.getName());
    writeFile(tmp, buildTokenData(tokenId, renewDate));
    try {
      deleteFile(tokenPath);
    } catch (IOException e) {
      fs.delete(tmp, false);
      throw e;
    }
    if (!fs.rename(tmp, tokenPath)) {
      throw new IOException("Could not rename " + tmp + " to " + tokenPath);
    }
  }

  /**
   * 删除已过期的代理令牌
   * @param tokenId 要删除的代理令牌标识符
   * @throws IOException 删除失败时抛出异常
   */
  @Override
  public void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing token " + tokenId.getSequenceNumber());
    }
    deleteFile(getTokenPath(tokenId));
  }

  /**
   * 存储新增的代理令牌主密钥
   * @param key 要存储的DelegationKey对象
   * @throws IOException 存储失败或密钥已存在时抛出异常
   */
  @Override
  public void storeTokenMasterKey(DelegationKey key) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing master key " + key.getKeyId());
    }

    Path keyPath = new Path(tokenKeysStatePath,
        TOKEN_MASTER_KEY_FILE_PREFIX + key.getKeyId());
    if (fs.exists(keyPath)) {
      throw new FileAlreadyExistsException(keyPath + " already exists");
    }

    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      key.write(dataStream);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }

    createNewFile(keyPath, memStream.toByteArray());
  }

  /**
   * 删除已过期的代理令牌主密钥
   * @param key 要删除的DelegationKey对象
   * @throws IOException 删除失败时抛出异常
   */
  @Override
  public void removeTokenMasterKey(DelegationKey key)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing master key " + key.getKeyId());
    }

    Path keyPath = new Path(tokenKeysStatePath,
        TOKEN_MASTER_KEY_FILE_PREFIX + key.getKeyId());
    deleteFile(keyPath);
  }

  private static int getBucketId(MRDelegationTokenIdentifier tokenId) {
    return tokenId.getSequenceNumber() % NUM_TOKEN_BUCKETS;
  }

  private Path getTokenBucketPath(int bucketId) {
    return new Path(tokenStatePath,
        String.format(TOKEN_BUCKET_NAME_FORMAT, bucketId));
  }

  private Path getTokenPath(MRDelegationTokenIdentifier tokenId) {
    Path bucketPath = getTokenBucketPath(getBucketId(tokenId));
    return new Path(bucketPath,
        TOKEN_FILE_PREFIX + tokenId.getSequenceNumber());
  }

  /**
   * 创建目录，如果目录已存在则检查并修正权限
   * @param dir 要创建的目录路径
   * @throws IOException 创建目录或设置权限失败时抛出异常
   */
  private void createDir(Path dir) throws IOException {
    try {
      FileStatus status = fs.getFileStatus(dir);
      if (!status.isDirectory()) {
        throw new FileAlreadyExistsException("Unexpected file in store: "
            + dir);
      }
      if (!status.getPermission().equals(DIR_PERMISSIONS)) {
        fs.setPermission(dir, DIR_PERMISSIONS);
      }
    } catch (FileNotFoundException e) {
      fs.mkdirs(dir, DIR_PERMISSIONS);
    }
  }

  /**
   * 通过临时文件原子创建新文件，避免创建过程中异常导致文件损坏
   * @param file 目标文件路径
   * @param data 要写入的文件数据
   * @throws IOException 创建或写入失败时抛出异常
   */
  private void createNewFile(Path file, byte[] data)
      throws IOException {
    Path tmp = new Path(file.getParent(), TMP_FILE_PREFIX + file.getName());
    writeFile(tmp, data);
    try {
      if (!fs.rename(tmp, file)) {
        throw new IOException("Could not rename " + tmp + " to " + file);
      }
    } catch (IOException e) {
      fs.delete(tmp, false);
      throw e;
    }
  }

  /**
   * 将字节数据写入指定路径的文件
   * @param file 目标文件路径
   * @param data 要写入的字节数据
   * @throws IOException 写入失败时抛出异常
   */
  private void writeFile(Path file, byte[] data) throws IOException {
    final int WRITE_BUFFER_SIZE = 4096;
    FSDataOutputStream out = fs.create(file, FILE_PERMISSIONS, true,
        WRITE_BUFFER_SIZE, fs.getDefaultReplication(file),
        fs.getDefaultBlockSize(file), null);
    try {
      try {
        out.write(data);
        out.close();
        out = null;
      } finally {
        IOUtils.cleanupWithLogger(LOG, out);
      }
    } catch (IOException e) {
      fs.delete(file, false);
      throw e;
    }
  }

  /**
   * 从指定路径读取固定长度的文件数据
   * @param file 要读取的文件路径
   * @param numBytes 要读取的字节数
   * @return 读取到的字节数组
   * @throws IOException 读取失败时抛出异常
   */
  private byte[] readFile(Path file, long numBytes) throws IOException {
    byte[] data = new byte[(int)numBytes];
    FSDataInputStream in = fs.open(file);
    try {
      in.readFully(data);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    return data;
  }

  /**
   * 删除指定文件，文件不存在时视为删除成功
   * @param file 要删除的文件路径
   * @throws IOException 删除失败时抛出异常
   */
  private void deleteFile(Path file) throws IOException {
    boolean deleted;
    try {
      deleted = fs.delete(file, false);
    } catch (FileNotFoundException e) {
      deleted = true;
    }
    if (!deleted) {
      throw new IOException("Unable to delete " + file);
    }
  }

  /**
   * 序列化令牌标识符和更新时间为字节数组
   * @param tokenId 代理令牌标识符
   * @param renewDate 令牌更新时间
   * @return 序列化后的字节数组
   * @throws IOException 序列化失败时抛出异常
   */
  private byte[] buildTokenData(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      tokenId.write(dataStream);
      dataStream.writeLong(renewDate);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }
    return memStream.toByteArray();
  }

  /**
   * 从文件加载单个代理令牌主密钥并添加到状态中
   * @param state 历史服务器状态对象
   * @param keyFile 主密钥文件路径
   * @param numKeyFileBytes 文件长度
   * @throws IOException 加载失败时抛出异常
   */
  private void loadTokenMasterKey(HistoryServerState state, Path keyFile,
      long numKeyFileBytes) throws IOException {
    DelegationKey key = new DelegationKey();
    byte[] keyData = readFile(keyFile, numKeyFileBytes);
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(keyData));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenMasterKeyState.add(key);
  }

  /**
   * 从分桶目录加载单个令牌，并校验分桶位置正确性
   * @param bucketId 当前分桶ID
   * @param state 历史服务器状态对象
   * @param tokenFile 令牌文件路径
   * @param numTokenFileBytes 文件长度
   * @throws IOException 加载失败或分桶错误时抛出异常
   */
  private void loadTokenFromBucket(int bucketId,
      HistoryServerState state, Path tokenFile, long numTokenFileBytes)
          throws IOException {
    MRDelegationTokenIdentifier token =
        loadToken(state, tokenFile, numTokenFileBytes);
    int tokenBucketId = getBucketId(token);
    if (tokenBucketId != bucketId) {
      throw new IOException("Token " + tokenFile
          + " should be in bucket " + tokenBucketId + ", found in bucket "
          + bucketId);
    }
  }

  /**
   * 从文件加载单个代理令牌并添加到状态中
   * @param state 历史服务器状态对象
   * @param tokenFile 令牌文件路径
   * @param numTokenFileBytes 文件长度
   * @return 加载完成的令牌标识符对象
   * @throws IOException 加载失败时抛出异常
   */
  private MRDelegationTokenIdentifier loadToken(HistoryServerState state,
      Path tokenFile, long numTokenFileBytes) throws IOException {
    MRDelegationTokenIdentifier tokenId = new MRDelegationTokenIdentifier();
    long renewDate;
    byte[] tokenData = readFile(tokenFile, numTokenFileBytes);
    DataInputStream in