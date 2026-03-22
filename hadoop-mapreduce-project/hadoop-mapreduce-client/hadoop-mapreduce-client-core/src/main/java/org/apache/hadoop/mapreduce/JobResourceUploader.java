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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.SharedCacheClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件级注释：MapReduce作业资源上传器，负责将客户端本地的作业相关资源（jar包、依赖文件、归档文件等）上传到HDFS集群的作业提交目录，
 * 同时支持使用YARN共享缓存优化资源复用，减少重复上传和存储开销。
 * 
 * This class is responsible for uploading resources from the client to HDFS
 * that are associated with a MapReduce job.
 */
@Private
@Unstable
class JobResourceUploader {
  protected static final Logger LOG =
      LoggerFactory.getLogger(JobResourceUploader.class);
  private static final String ROOT_PATH = "/";

  private final boolean useWildcard;
  private final FileSystem jtFs;
  private SharedCacheClient scClient = null;
  private SharedCacheConfig scConfig = new SharedCacheConfig();
  private ApplicationId appId = null;

  /**
   * 构造方法，初始化作业资源上传器
   * @param submitFs 集群作业提交目标文件系统
   * @param useWildcard 是否对libjars使用通配符方式批量添加到分布式缓存
   */
  JobResourceUploader(FileSystem submitFs, boolean useWildcard) {
    this.jtFs = submitFs;
    this.useWildcard = useWildcard;
  }

  /**
   * 初始化YARN共享缓存客户端，若配置开启共享缓存则创建客户端并转换作业ID为应用ID
   * @param jobid 当前MapReduce作业ID
   * @param conf 作业配置对象
   */
  private void initSharedCache(JobID jobid, Configuration conf) {
    this.scConfig.init(conf);
    if (this.scConfig.isSharedCacheEnabled()) {
      this.scClient = createSharedCacheClient(conf);
      appId = jobIDToAppId(jobid);
    }
  }

  /*
   * We added this method so that we could do the conversion between JobId and
   * ApplicationId for the shared cache client. This logic is very similar to
   * the org.apache.hadoop.mapreduce.TypeConverter#toYarn method. We don't use
   * that because mapreduce-client-core can not depend on
   * mapreduce-client-common.
   */
  /**
   * 将MapReduce JobID转换为YARN ApplicationId，用于共享缓存客户端调用
   * @param jobId MapReduce作业ID
   * @return 对应的YARN应用ID
   */
  private ApplicationId jobIDToAppId(JobID jobId) {
    return ApplicationId.newInstance(Long.parseLong(jobId.getJtIdentifier()),
        jobId.getId());
  }

  /**
   * 停止并清空共享缓存客户端，释放资源
   */
  private void stopSharedCache() {
    if (scClient != null) {
      scClient.stop();
      scClient = null;
    }
  }

  /**
   * Create, initialize and start a new shared cache client.
   */
  @VisibleForTesting
  /**
   * 创建并启动共享缓存客户端，暴露用于测试
   * @param conf 配置对象
   * @return 初始化完成的共享缓存客户端
   */
  protected SharedCacheClient createSharedCacheClient(Configuration conf) {
    SharedCacheClient scc = SharedCacheClient.createSharedCacheClient();
    scc.init(conf);
    scc.start();
    return scc;
  }

  /**
   * 上传作业所有相关资源（普通文件、libjars、归档文件、作业jar包）到作业提交目录，支持共享缓存复用
   * <p>
   * This client will use the shared cache for libjars, files, archives and
   * jobjars if it is enabled. When shared cache is enabled, it will try to use
   * the shared cache and fall back to the default behavior when the scm isn't
   * available.
   * <p>
   * 1. For the resources that have been successfully shared, we will continue
   * to use them in a shared fashion.
   * <p>
   * 2. For the resources that weren't in the cache and need to be uploaded by
   * NM, we won't ask NM to upload them.
   *
   * @param job 要上传资源的作业对象
   * @param submitJobDir 作业在HDFS上的提交目录
   * @throws IOException 上传过程中IO异常
   */
  public void uploadResources(Job job, Path submitJobDir) throws IOException {
    try {
      initSharedCache(job.getJobID(), job.getConfiguration());
      uploadResourcesInternal(job, submitJobDir);
    } finally {
      stopSharedCache();
    }
  }

  /**
   * 内部执行资源上传的核心逻辑，处理各类资源的上传和配置
   * @param job 作业对象
   * @param submitJobDir 作业提交目录
   * @throws IOException 上传过程中IO异常
   */
  private void uploadResourcesInternal(Job job, Path submitJobDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    short replication =
        (short) conf.getInt(Job.SUBMIT_REPLICATION,
            Job.DEFAULT_SUBMIT_REPLICATION);

    if (!(conf.getBoolean(Job.USED_GENERIC_PARSER, false))) {
      LOG.warn("Hadoop command-line option parsing not performed. "
          + "Implement the Tool interface and execute your application "
          + "with ToolRunner to remedy this.");
    }

    //
    // Figure out what fs the JobTracker is using. Copy the
    // job to it, under a temporary name. This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading
    // semantics. (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    LOG.debug("default FileSystem: " + jtFs.getUri());
    if (jtFs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          + " already exists!! This is unexpected.Please check what's there in"
          + " that directory");
    }
    // 创建作业提交目录并标准化路径
    submitJobDir = jtFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission mapredSysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    mkdirs(jtFs, submitJobDir, mapredSysPerms);

    if (!conf.getBoolean(MRJobConfig.MR_AM_STAGING_DIR_ERASURECODING_ENABLED,
        MRJobConfig.DEFAULT_MR_AM_STAGING_ERASURECODING_ENABLED)) {
      disableErasureCodingForPath(submitJobDir);
    }

    // 获取命令行参数指定的各类资源
    Collection<String> files = conf.getStringCollection("tmpfiles");
    Collection<String> libjars = conf.getStringCollection("tmpjars");
    Collection<String> archives = conf.getStringCollection("tmparchives");
    String jobJar = job.getJar();

    // 合并通过Job API编程方式添加的共享缓存资源
    files.addAll(conf.getStringCollection(MRJobConfig.FILES_FOR_SHARED_CACHE));
    libjars.addAll(conf.getStringCollection(
            MRJobConfig.FILES_FOR_CLASSPATH_AND_SHARED_CACHE));
    archives.addAll(conf
        .getStringCollection(MRJobConfig.ARCHIVES_FOR_SHARED_CACHE));


    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    // 检查所有资源是否满足大小和数量限制
    checkLocalizationLimits(conf, files, libjars, archives, jobJar, statCache);

    Map<String, Boolean> fileSCUploadPolicies =
        new LinkedHashMap<String, Boolean>();
    Map<String, Boolean> archiveSCUploadPolicies =
        new LinkedHashMap<String, Boolean>();

    // 依次上传各类资源
    uploadFiles(job, files, submitJobDir, mapredSysPerms, replication,
        fileSCUploadPolicies, statCache);
    uploadLibJars(job, libjars, submitJobDir, mapredSysPerms, replication,
        fileSCUploadPolicies, statCache);
    uploadArchives(job, archives, submitJobDir, mapredSysPerms, replication,
        archiveSCUploadPolicies, statCache);
    uploadJobJar(job, jobJar, submitJobDir, replication, statCache);
    addLog4jToDistributedCache(job, submitJobDir);

    // Note, we do not consider resources in the distributed cache for the
    // shared cache at this time. Only resources specified via the
    // GenericOptionsParser or the jobjar.
    // 保存共享缓存上传策略到配置
    Job.setFileSharedCacheUploadPolicies(conf, fileSCUploadPolicies);
    Job.setArchiveSharedCacheUploadPolicies(conf, archiveSCUploadPolicies);

    // 设置缓存资源时间戳和可见性
    ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(conf,
        statCache);
    // 获取缓存文件的委托令牌用于认证
    ClientDistributedCacheManager.getDelegationTokens(conf,
        job.getCredentials());
  }

  @VisibleForTesting
  /**
   * 上传普通文件资源到分布式缓存，尝试复用共享缓存
   * @param job 作业对象
   * @param files 待上传文件路径集合
   * @param submitJobDir 作业提交目录
   * @param mapredSysPerms 目录权限
   * @param submitReplication 文件副本数
   * @param fileSCUploadPolicies 保存文件共享缓存上传策略
   * @param statCache 文件状态缓存
   * @throws IOException 上传IO异常
   */
  void uploadFiles(Job job, Collection<String> files,
      Path submitJobDir, FsPermission mapredSysPerms, short submitReplication,
      Map<String, Boolean> fileSCUploadPolicies, Map<URI, FileStatus> statCache)
      throws IOException {
    Configuration conf = job.getConfiguration();
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
    if (!files.isEmpty()) {
      mkdirs(jtFs, filesDir, mapredSysPerms);
      for (String tmpFile : files) {
        URI tmpURI = null;
        try {
          tmpURI = new URI(tmpFile);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Error parsing files argument."
              + " Argument must be a valid URI: " + tmpFile, e);
        }
        Path tmp = new Path(tmpURI);
        URI newURI = null;
        boolean uploadToSharedCache = false;
        if (scConfig.isSharedCacheFilesEnabled()) {
          newURI = useSharedCache(tmpURI, tmp.getName(), statCache, conf, true);
          if (newURI == null) {
            uploadToSharedCache = true;
          }
        }

        if (newURI == null) {
          Path newPath =
              copyRemoteFiles(filesDir, tmp, conf, submitReplication);
          try {
            newURI = getPathURI(newPath, tmpURI.getFragment());
          } catch (URISyntaxException ue) {
            // should not throw a uri exception
            throw new IOException(
                "Failed to create a URI (URISyntaxException) for the"
                    + " remote path " + newPath
                    + ". This was based on the files parameter: " + tmpFile,
                ue);
          }
        }

        job.addCacheFile(newURI);
        if (scConfig.isSharedCacheFilesEnabled()) {
          fileSCUploadPolicies.put(newURI.toString(), uploadToSharedCache);
        }
      }
    }
  }

  // Suppress warning for use of DistributedCache (it is everywhere).
  @SuppressWarnings("deprecation")
  @VisibleForTesting
  /**
   * 上传依赖jar包资源，添加到类路径和分布式缓存，支持通配符批量添加和共享缓存复用
   * @param job 作业对象
   * @param libjars 待上传jar路径集合
   * @param submitJobDir 作业提交目录
   * @param mapredSysPerms 目录权限
   * @param submitReplication 文件副本数
   * @param fileSCUploadPolicies 保存共享缓存上传策略
   * @param statCache 文件状态缓存
   * @throws IOException 上传IO异常
   */
  void uploadLibJars(Job job, Collection<String> libjars, Path submitJobDir,
      FsPermission mapredSysPerms, short submitReplication,
      Map<String, Boolean> fileSCUploadPolicies, Map<URI, FileStatus> statCache)
      throws IOException {
    Configuration conf = job.getConfiguration();
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);
    if (!libjars.isEmpty()) {
      mkdirs(jtFs, libjarsDir, mapredSysPerms);
      Collection<URI> libjarURIs = new LinkedList<>();
      boolean foundFragment = false;
      for (String tmpjars : libjars) {
        URI tmpURI = null;
        try {
          tmpURI = new URI(tmpjars);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Error parsing libjars argument."
              + " Argument must be a valid URI: " + tmpjars, e);
        }
        Path tmp = new Path(tmpURI);
        URI newURI = null;
        boolean uploadToSharedCache = false;
        boolean fromSharedCache = false;
        if (scConfig.isSharedCacheLibjarsEnabled()) {
          newURI = useSharedCache(tmpURI, tmp.getName(), statCache, conf, true);
          if (newURI == null) {
            uploadToSharedCache = true;
          } else {
            fromSharedCache = true;
          }
        }

        if (newURI == null) {
          Path newPath =
              copyRemoteFiles(libjarsDir, tmp, conf, submitReplication);
          try {
            newURI = getPathURI(newPath, tmpURI.getFragment());
          } catch (URISyntaxException ue) {
            // should not throw a uri exception
            throw new IOException(
                "Failed to create a URI (URISyntaxException) for the"
                    + " remote path " + newPath
                    + ". This was based on the libjar parameter: " + tmpjars,
                ue);
          }
        }

        if (!foundFragment) {
          // We do not count shared cache paths containing fragments as a
          // "foundFragment." This is because these resources are not in the
          // staging directory and will be added to the distributed cache
          // separately.
          foundFragment = (newURI.getFragment() != null) && !fromSharedCache;
        }
        Job.addFileToClassPath(new Path(newURI.getPath()), conf, jtFs, false);
        if (fromSharedCache) {
          // We simply add this URI to the distributed cache. It will not come
          // from the staging directory (it is in the shared cache), so we
          // must add it to the cache regardless of the wildcard feature.
          Job.addCacheFile(newURI, conf);
        } else {
          libjarURIs.add(newURI);
        }

        if (scConfig.isSharedCacheLibjarsEnabled()) {
          fileSCUploadPolicies.put(newURI.toString(), uploadToSharedCache