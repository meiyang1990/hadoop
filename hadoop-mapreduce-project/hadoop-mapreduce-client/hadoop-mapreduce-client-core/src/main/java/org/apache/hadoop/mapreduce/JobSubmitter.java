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
package org.apache.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.QueueACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;

import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.JsonSerialization;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ReservationId;

/**
 * MapReduce作业提交器，负责将用户作业提交到YARN集群的核心流程处理
 * 包含作业参数校验、资源上传、分片计算、作业提交等完整提交流程
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobSubmitter {
  protected static final Logger LOG =
      LoggerFactory.getLogger(JobSubmitter.class);
  private static final String SHUFFLE_KEYGEN_ALGORITHM = "HmacSHA1";
  private FileSystem jtFs;
  private ClientProtocol submitClient;
  private String submitHostName;
  private String submitHostAddress;
  
  /**
   * 构造作业提交器实例
   * @param submitFs 提交作业使用的文件系统
   * @param submitClient 与集群通信的客户端协议
   * @throws IOException 初始化异常
   */
  JobSubmitter(FileSystem submitFs, ClientProtocol submitClient) 
  throws IOException {
    this.submitClient = submitClient;
    this.jtFs = submitFs;
  }
  
  /**
   * 上传作业依赖资源（libjars、files、archives）并配置分布式缓存
   * @param job 待提交作业对象
   * @param jobSubmitDir 作业提交临时目录
   * @throws IOException 文件操作异常
   */
  private void copyAndConfigureFiles(Job job, Path jobSubmitDir) 
  throws IOException {
    Configuration conf = job.getConfiguration();
    boolean useWildcards = conf.getBoolean(Job.USE_WILDCARD_FOR_LIBJARS,
        Job.DEFAULT_USE_WILDCARD_FOR_LIBJARS);
    JobResourceUploader rUploader = new JobResourceUploader(jtFs, useWildcards);

    rUploader.uploadResources(job, jobSubmitDir);

    // 获取工作目录，未设置时使用文件系统默认工作目录
    // 为了向后兼容，保证作业运行前工作目录会被重置
    job.getWorkingDirectory();
  }

  /**
   * 作业提交核心内部方法，完成作业从参数校验到实际提交的全流程
   * 流程包括：参数校验、输入分片计算、分布式缓存配置、资源上传、作业提交
   * @param job 待提交作业对象
   * @param cluster 集群连接句柄
   * @return 提交成功后的作业状态对象
   * @throws ClassNotFoundException 类找不到异常
   * @throws InterruptedException 中断异常
   * @throws IOException IO异常
   */
  JobStatus submitJobInternal(Job job, Cluster cluster) 
  throws ClassNotFoundException, InterruptedException, IOException {

    // 校验作业输入输出规范
    checkSpecs(job);

    Configuration conf = job.getConfiguration();
    // 添加MapReduce框架路径到分布式缓存
    addMRFrameworkToDistributedCache(conf);

    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);
    // 获取提交节点信息记录到配置
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      submitHostAddress = ip.getHostAddress();
      submitHostName = ip.getHostName();
      conf.set(MRJobConfig.JOB_SUBMITHOST,submitHostName);
      conf.set(MRJobConfig.JOB_SUBMITHOSTADDR,submitHostAddress);
    }
    // 从服务端获取新的作业ID
    JobID jobId = submitClient.getNewJobID();
    job.setJobID(jobId);
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    JobStatus status = null;
    try {
      // 设置当前提交用户名
      conf.set(MRJobConfig.USER_NAME,
          UserGroupInformation.getCurrentUser().getShortUserName());
      // 设置代理过滤器初始化器
      conf.set("hadoop.http.filter.initializers", 
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      // 设置作业提交目录
      conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, submitJobDir.toString());
      LOG.debug("Configuring job " + jobId + " with " + submitJobDir 
          + " as the submit dir");
      // 获取提交目录的委托令牌
      TokenCache.obtainTokensForNamenodes(job.getCredentials(),
          new Path[] { submitJobDir }, conf);
      
      // 从文件加载令牌和密钥填充到TokenCache
      populateTokenCache(conf, job.getCredentials());

      // 若不存在shuffle认证密钥则生成
      if (TokenCache.getShuffleSecretKey(job.getCredentials()) == null) {
        KeyGenerator keyGen;
        try {
          keyGen = KeyGenerator.getInstance(SHUFFLE_KEYGEN_ALGORITHM);
          int shuffleKeyLength =
              conf.getInt(MRJobConfig.SHUFFLE_KEY_LENGTH, MRJobConfig.DEFAULT_SHUFFLE_KEY_LENGTH);
          keyGen.init(shuffleKeyLength);
        } catch (NoSuchAlgorithmException e) {
          throw new IOException("Error generating shuffle secret key", e);
        }
        SecretKey shuffleKey = keyGen.generateKey();
        TokenCache.setShuffleSecretKey(shuffleKey.getEncoded(),
            job.getCredentials());
      }
      // 开启加密溢写时强制将最大尝试次数设为1
      if (CryptoUtils.isEncryptedSpillEnabled(conf)) {
        conf.setInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, 1);
        LOG.warn("Max job attempts set to 1 since encrypted intermediate" +
                "data spill is enabled");
      }

      // 上传作业依赖资源并配置
      copyAndConfigureFiles(job, submitJobDir);

      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
      
      // 为作业创建输入分片
      LOG.debug("Creating splits at " + jtFs.makeQualified(submitJobDir));
      int maps = writeSplits(job, submitJobDir);
      conf.setInt(MRJobConfig.NUM_MAPS, maps);
      LOG.info("number of splits:" + maps);

      // 检查分片数量不超过最大限制
      int maxMaps = conf.getInt(MRJobConfig.JOB_MAX_MAP,
          MRJobConfig.DEFAULT_JOB_MAX_MAP);
      if (maxMaps >= 0 && maxMaps < maps) {
        throw new IllegalArgumentException("The number of map tasks " + maps +
            " exceeded limit " + maxMaps);
      }

      // 获取提交队列管理员ACL，写入配置
      String queue = conf.get(MRJobConfig.QUEUE_NAME,
          JobConf.DEFAULT_QUEUE_NAME);
      AccessControlList acl = submitClient.getQueueAdmins(queue);
      conf.set(toFullPropertyName(queue,
          QueueACL.ADMINISTER_JOBS.getAclName()), acl.getAclString());

      // 清理令牌引用，避免任务运行出错
      TokenCache.cleanUpTokenReferral(conf);

      // 若开启令牌跟踪ID，则收集所有令牌跟踪ID写入配置
      if (conf.getBoolean(
          MRJobConfig.JOB_TOKEN_TRACKING_IDS_ENABLED,
          MRJobConfig.DEFAULT_JOB_TOKEN_TRACKING_IDS_ENABLED)) {
        ArrayList<String> trackingIds = new ArrayList<String>();
        for (Token<? extends TokenIdentifier> t :
            job.getCredentials().getAllTokens()) {
          trackingIds.add(t.decodeIdentifier().getTrackingId());
        }
        conf.setStrings(MRJobConfig.JOB_TOKEN_TRACKING_IDS,
            trackingIds.toArray(new String[trackingIds.size()]));
      }

      // 若存在预约ID则写入配置
      ReservationId reservationId = job.getReservationId();
      if (reservationId != null) {
        conf.set(MRJobConfig.RESERVATION_ID, reservationId.toString());
      }

      // 将作业配置写入提交目录
      writeConf(conf, submitJobFile);
      
      // 打印提交的令牌信息
      printTokens(jobId, job.getCredentials());
      // 实际提交作业到服务端
      status = submitClient.submitJob(
          jobId, submitJobDir.toString(), job.getCredentials());
      if (status != null) {
        return status;
      } else {
        throw new IOException("Could not launch job");
      }
    } finally {
      // 提交失败清理临时 staging 目录
      if (status == null) {
        LOG.info("Cleaning up the staging area " + submitJobDir);
        if (jtFs != null && submitJobDir != null)
          jtFs.delete(submitJobDir, true);

      }
    }
  }
  
  /**
   * 校验作业输出规范
   * @param job 待校验作业对象
   * @throws ClassNotFoundException 类找不到异常
   * @throws InterruptedException 中断异常
   * @throws IOException IO异常
   */
  private void checkSpecs(Job job) throws ClassNotFoundException, 
      InterruptedException, IOException {
    JobConf jConf = (JobConf)job.getConfiguration();
    // 根据新旧API选择对应输出格式校验
    if (jConf.getNumReduceTasks() == 0 ? 
        jConf.getUseNewMapper() : jConf.getUseNewReducer()) {
      org.apache.hadoop.mapreduce.OutputFormat<?, ?> output =
        ReflectionUtils.newInstance(job.getOutputFormatClass(),
          job.getConfiguration());
      output.checkOutputSpecs(job);
    } else {
      jConf.getOutputFormat().checkOutputSpecs(jtFs, jConf);
    }
  }
  
  /**
   * 将作业配置写入XML文件到HDFS
   * @param conf 作业配置对象
   * @param jobFile 输出文件路径
   * @throws IOException IO异常
   */
  private void writeConf(Configuration conf, Path jobFile) 
      throws IOException {
    // 创建输出流写入配置
    FSDataOutputStream out = 
      FileSystem.create(jtFs, jobFile, 
                        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }
  
  /**
   * 打印提交作业携带的令牌信息日志
   * @param jobId 作业ID
   * @param credentials 凭证对象
   * @throws IOException IO异常
   */
  private void printTokens(JobID jobId,
      Credentials credentials) throws IOException {
    LOG.info("Submitting tokens for job: " + jobId);
    LOG.info("Executing with tokens: {}", credentials.getAllTokens());
  }

  @SuppressWarnings("unchecked")
  /**
   * 使用新API输入格式计算并写入输入分片
   * @param job 作业上下文
   * @param jobSubmitDir 作业提交目录
   * @return 分片数量（即Map任务数量）
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   * @throws ClassNotFoundException 类找不到异常
   */
  private <T extends InputSplit>
  int writeNewSplits(JobContext job, Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    InputFormat<?, ?> input =
      ReflectionUtils.newInstance(job.getInputFormatClass(), conf);

    List<InputSplit> splits = input.getSplits(job);
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);

    // 按分片大小降序排序，大分片优先处理
    Arrays.sort(array, new SplitComparator());
    JobSplitWriter.createSplitFiles(jobSubmitDir, conf, 
        jobSubmitDir.getFileSystem(conf), array);
    return array.length;
  }
  
  /**
   * 根据API版本选择对应分片写入方法
   * @param job 作业上下文
   * @param jobSubmitDir 作业提交目录
   * @return 分片数量（即Map任务数量）
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   * @throws ClassNotFoundException 类找不到异常
   */
  private int writeSplits(org.apache.hadoop.mapreduce.JobContext job,
      Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    JobConf jConf = (JobConf)job.getConfiguration();
    int maps;
    if (jConf.getUseNewMapper()) {
      maps = writeNewSplits(job, jobSubmitDir);
    } else {
      maps = writeOldSplits(jConf, jobSubmitDir);
    }
    return maps;
  }
  
  /**
   * 使用旧API输入格式计算并写入输入分片
   * @param job 旧API作业配置
   * @param jobSubmitDir 作业提交目录
   * @return 分片数量（即Map任务数量）
   * @throws IOException IO异常
   */
  private int writeOldSplits(JobConf job, Path jobSubmitDir) 
  throws IOException {
    org.apache.hadoop.mapred.InputSplit[] splits =
    job.getInputFormat().getSplits(job, job.getNumMapTasks());
    // 按分片大小降序排序，大分片优先处理
    Arrays.sort(splits, new Comparator<org.apache.hadoop.mapred.InputSplit>() {
      public int compare(org.apache.hadoop.mapred.InputSplit a,
                         org.apache.hadoop.mapred.InputSplit b) {
        try {
          long left = a.getLength();
          long right = b.getLength();
          if (left == right) {
            return 0;
          } else if (left < right) {
            return 1;
          } else {
            return -1;
          }
        } catch (IOException ie) {
          throw new RuntimeException("Problem getting input split size", ie);
        }
      }
    });
    JobSplitWriter.createSplitFiles(jobSubmitDir, job, 
        jobSubmitDir.getFileSystem(job), splits);
    return splits.length;
  }
  
  /**
   * 输入分片比较器，按分片大小降序排序
   */
  private static class SplitComparator implements Comparator<InputSplit> {
    @Override
    public int compare(InputSplit o1, InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in compare", ie);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  /**
   * 从本地文件读取令牌和密钥添加到凭证
   * 支持二进制格式和JSON格式两种凭据文件
   * @param conf 作业配置
   * @param credentials 目标凭证对象
   * @throws IOException IO异常
   */
  private void readTokensFromFiles(Configuration conf, Credentials credentials)
  throws IOException {
    // 添加二进制令牌文件中的凭据
    String binaryTokenFilename =
      conf.get(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
    if (binaryTokenFilename != null) {
      Credentials binary = Credentials.readTokenStorageFile(
          FileSystem.getLocal(conf).makeQualified(
              new Path