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

package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TokenCache工具类，提供从作业客户端向任务传递安全凭证（密钥、令牌）的能力
 * 支持作业提交前存储凭证，任务执行时读取凭证，用于MapReduce任务安全认证
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TokenCache {
  
  private static final Logger LOG = LoggerFactory.getLogger(TokenCache.class);

  /**
   * 从凭证中获取指定别名的密钥
   * @param credentials 凭证存储对象
   * @param alias 密钥别名
   * @return 对应的密钥字节数组，若凭证为空则返回null
   */
  public static byte[] getSecretKey(Credentials credentials, Text alias) {
    if(credentials == null)
      return null;
    return credentials.getSecretKey(alias);
  }
  
  /**
   * 为输入路径对应的所有NameNode获取委托令牌，并存储到凭证中
   * @param credentials 存储令牌的凭证对象
   * @param ps 作业输入/输出路径数组
   * @param conf 作业配置
   * @throws IOException 获取令牌失败时抛出异常
   */
  public static void obtainTokensForNamenodes(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      // 安全模式未开启，无需获取令牌，直接返回
      return;
    }
    obtainTokensForNamenodesInternal(credentials, ps, conf);
  }

  /**
   * 清除配置中存放凭证二进制文件的引用，避免任务执行上下文出现无效引用
   * @param conf 作业配置对象
   */
  public static void cleanUpTokenReferral(Configuration conf) {
    conf.unset(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
  }

  /**
   * 内部方法：遍历所有路径对应的文件系统，逐个获取NameNode委托令牌
   * @param credentials 存储令牌的凭证对象
   * @param ps 路径数组
   * @param conf 作业配置
   * @throws IOException 获取令牌失败时抛出异常
   */
  static void obtainTokensForNamenodesInternal(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    Set<FileSystem> fsSet = new HashSet<FileSystem>();
    // 去重获取所有路径对应的文件系统实例
    for(Path p: ps) {
      fsSet.add(p.getFileSystem(conf));
    }
    String masterPrincipal = Master.getMasterPrincipal(conf);
    // 为每个文件系统获取委托令牌
    for (FileSystem fs : fsSet) {
      obtainTokensForNamenodesInternal(fs, credentials, conf, masterPrincipal);
    }
  }

  /**
   * 判断指定文件系统对应的NameNode是否需要排除令牌自动续期
   * @param fs 目标文件系统
   * @param conf 作业配置
   * @return true表示排除续期，false表示需要续期
   */
  static boolean isTokenRenewalExcluded(FileSystem fs, Configuration conf) {
    String [] nns =
        conf.getStrings(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE);
    if (nns != null) {
      // 获取当前文件系统主机名
      String host = fs.getUri().getHost();
      // 匹配排除列表
      for(int i=0; i< nns.length; i++) {
        if (nns[i].equals(host)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * 内部方法：为指定文件系统获取委托令牌，合并外部凭证二进制文件后存储到凭证对象
   * @param fs 目标文件系统
   * @param credentials 存储令牌的凭证对象
   * @param conf 作业配置
   * @param renewer 令牌续用户主体名称
   * @throws IOException 获取令牌失败时抛出异常
   */
  static void obtainTokensForNamenodesInternal(FileSystem fs,
      Credentials credentials, Configuration conf, String renewer)
      throws IOException {
    // RM会跳过空续用户的令牌续期，这里默认空表示排除续期
    String delegTokenRenewer = "";
    // 当前NameNode不需要排除续期
    if (!isTokenRenewalExcluded(fs, conf)) {
      if (StringUtils.isEmpty(renewer)) {
        // 未获取到RM主体，无法续期，抛出异常
        throw new IOException(
            "Can't get Master Kerberos principal for use as renewer");
      } else {
        // 设置续用户为RM主体
        delegTokenRenewer = renewer;
      }
    }

    // 合并二进制文件中的凭证到当前凭证对象
    mergeBinaryTokens(credentials, conf);

    final Token<?> tokens[] = fs.addDelegationTokens(delegTokenRenewer,
                                                     credentials);
    if (tokens != null) {
      for (Token<?> token : tokens) {
        LOG.info("Got dt for " + fs.getUri() + "; "+token);
      }
    }
  }

  /**
   * 私有方法：读取配置指定的二进制凭证文件，合并所有令牌到目标凭证对象
   * @param creds 目标凭证对象
   * @param conf 作业配置
   */
  private static void mergeBinaryTokens(Credentials creds, Configuration conf) {
    String binaryTokenFilename =
        conf.get(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
    if (binaryTokenFilename != null) {
      Credentials binary;
      try {
        // 读取本地文件系统中的二进制凭证文件
        binary = Credentials.readTokenStorageFile(
            FileSystem.getLocal(conf).makeQualified(
                new Path(binaryTokenFilename)),
            conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // 将二进制文件中的凭证合并到现有凭证中
      creds.mergeAll(binary);
    }
  }
  
  /**
   * HDFS上存储作业令牌的文件名
   */
  @InterfaceAudience.Private
  public static final String JOB_TOKEN_HDFS_FILE = "jobToken";

  /**
   * 配置项：作业令牌缓存文件路径
   */
  @InterfaceAudience.Private
  public static final String JOB_TOKENS_FILENAME = "mapreduce.job.jobTokenFile";
  private static final Text JOB_TOKEN = new Text("JobToken");
  private static final Text SHUFFLE_TOKEN = new Text("MapReduceShuffleToken");
  private static final Text ENC_SPILL_KEY = new Text("MapReduceEncryptedSpillKey");
  
  /**
   * 从文件加载作业令牌，兼容Hadoop 1.x版本
   * @deprecated 请使用 {@link Credentials#readTokenStorageFile} 替代
   * @param jobTokenFile 作业令牌文件路径
   * @param conf 作业配置
   * @return 加载后的凭证对象
   * @throws IOException 读取文件失败时抛出异常
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Credentials loadTokens(String jobTokenFile, JobConf conf)
  throws IOException {
    Path localJobTokenFile = new Path ("file:///" + jobTokenFile);

    Credentials ts = Credentials.readTokenStorageFile(localJobTokenFile, conf);

    if(LOG.isDebugEnabled()) {
      LOG.debug("Task: Loaded jobTokenFile from: "+
          localJobTokenFile.toUri().getPath() 
          +"; num of sec keys  = " + ts.numberOfSecretKeys() +
          " Number of tokens " +  ts.numberOfTokens());
    }
    return ts;
  }
  
  /**
   * 从文件加载作业令牌，兼容Hadoop 1.x版本
   * @deprecated 请使用 {@link Credentials#readTokenStorageFile} 替代
   * @param jobTokenFile 作业令牌文件路径
   * @param conf 作业配置
   * @return 加载后的凭证对象
   * @throws IOException 读取文件失败时抛出异常
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Credentials loadTokens(String jobTokenFile, Configuration conf)
      throws IOException {
    return loadTokens(jobTokenFile, new JobConf(conf));
  }
  
  /**
   * 将作业令牌存储到凭证对象中
   * @param t 作业令牌对象
   * @param credentials 目标凭证对象
   */
  @InterfaceAudience.Private
  public static void setJobToken(Token<? extends TokenIdentifier> t, 
      Credentials credentials) {
    credentials.addToken(JOB_TOKEN, t);
  }

  /**
   * 从凭证对象中获取作业令牌
   * @param credentials 凭证对象
   * @return 作业令牌对象
   */
  @SuppressWarnings("unchecked")
  @InterfaceAudience.Private
  public static Token<JobTokenIdentifier> getJobToken(Credentials credentials) {
    return (Token<JobTokenIdentifier>) credentials.getToken(JOB_TOKEN);
  }

  /**
   * 将Shuffle认证密钥存储到凭证对象中
   * @param key Shuffle密钥字节数组
   * @param credentials 目标凭证对象
   */
  @InterfaceAudience.Private
  public static void setShuffleSecretKey(byte[] key, Credentials credentials) {
    credentials.addSecretKey(SHUFFLE_TOKEN, key);
  }

  /**
   * 从凭证对象中获取Shuffle认证密钥
   * @param credentials 凭证对象
   * @return Shuffle密钥字节数组
   */
  @InterfaceAudience.Private
  public static byte[] getShuffleSecretKey(Credentials credentials) {
    return getSecretKey(credentials, SHUFFLE_TOKEN);
  }

  /**
   * 将溢写加密密钥存储到凭证对象中
   * @param key 加密密钥字节数组
   * @param credentials 目标凭证对象
   */
  @InterfaceAudience.Private
  public static void setEncryptedSpillKey(byte[] key, Credentials credentials) {
    credentials.addSecretKey(ENC_SPILL_KEY, key);
  }

  /**
   * 从凭证对象中获取溢写加密密钥
   * @param credentials 凭证对象
   * @return 加密密钥字节数组
   */
  @InterfaceAudience.Private
  public static byte[] getEncryptedSpillKey(Credentials credentials) {
    return getSecretKey(credentials, ENC_SPILL_KEY);
  }

  /**
   * 从凭证中获取指定NameNode的委托令牌，兼容Hadoop 1.x版本
   * @deprecated 请使用 {@link Credentials#getToken(org.apache.hadoop.io.Text)} 替代
   * @param credentials 凭证对象
   * @param namenode NameNode标识
   * @return 对应的委托令牌
   */
  @InterfaceAudience.Private
  @Deprecated
  public static
      Token<?> getDelegationToken(
          Credentials credentials, String namenode) {
    return (Token<?>) credentials.getToken(new Text(
      namenode));
  }
}