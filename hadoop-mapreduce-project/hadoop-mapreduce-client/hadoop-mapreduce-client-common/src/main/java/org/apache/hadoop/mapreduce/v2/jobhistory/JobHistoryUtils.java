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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业历史工具类，提供作业历史文件路径构造、权限管理、目录扫描清理等公共能力
 * 为MapReduce作业历史服务提供统一的路径格式、权限规则和文件查找工具
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobHistoryUtils {
  
  /**
   * Permissions for the history staging dir while JobInProgress.
   */
  public static final FsPermission HISTORY_STAGING_DIR_PERMISSIONS =
    
    FsPermission.createImmutable( (short) 0700);
  
  /**
   * Permissions for the user directory under the staging directory.
   */
  public static final FsPermission HISTORY_STAGING_USER_DIR_PERMISSIONS = 
    FsPermission.createImmutable((short) 0700);
  
  
  
  /**
   * Permissions for the history done dir and derivatives.
   */
  public static final FsPermission HISTORY_DONE_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0770); 

  public static final FsPermission HISTORY_DONE_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0770); // rwx------

 /**
   * Umask for the done dir and derivatives.
   */
  public static final FsPermission HISTORY_DONE_DIR_UMASK = FsPermission
      .createImmutable((short) (0770 ^ 0777));

  
  /**
   * Permissions for the intermediate done directory.
   */
  public static final FsPermission HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS = 
    FsPermission.createImmutable((short) 01777);

  /**
   * Suffix for configuration files.
   */
  public static final String CONF_FILE_NAME_SUFFIX = "_conf.xml";
  
  /**
   * Suffix for summary files.
   */
  public static final String SUMMARY_FILE_NAME_SUFFIX = ".summary";
  
  /**
   * Job History File extension.
   */
  public static final String JOB_HISTORY_FILE_EXTENSION = ".jhist";
  
  public static final int VERSION = 4;

  public static final int SERIAL_NUMBER_DIRECTORY_DIGITS = 6;
  
  public static final String TIMESTAMP_DIR_REGEX = "\\d{4}" + "\\" + Path.SEPARATOR +  "\\d{2}" + "\\" + Path.SEPARATOR + "\\d{2}";
  public static final Pattern TIMESTAMP_DIR_PATTERN = Pattern.compile(TIMESTAMP_DIR_REGEX);
  private static final String TIMESTAMP_DIR_FORMAT = "%04d" + File.separator + "%02d" + File.separator + "%02d";
  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryUtils.class);

  // 配置文件路径过滤器
  private static final PathFilter CONF_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(CONF_FILE_NAME_SUFFIX);
    }
  };
  
  // 作业历史文件路径过滤器
  private static final PathFilter JOB_HISTORY_FILE_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(JOB_HISTORY_FILE_EXTENSION);
    }
  };

  /**
   * 检查给定路径字符串是否为有效的作业历史文件名
   * @param pathString 待检查的路径
   * @return 有效返回true，否则返回false
   */
  public static boolean isValidJobHistoryFileName(String pathString) {
    return pathString.endsWith(JOB_HISTORY_FILE_EXTENSION);
  }

  /**
   * 从作业历史文件路径中解析提取JobID
   * @param pathString 作业历史文件路径字符串
   * @return 解析得到的JobID对象
   * @throws IOException 如果文件名格式无效则抛出异常
   */
  public static JobID getJobIDFromHistoryFilePath(String pathString) throws IOException {
    String [] parts = pathString.split(Path.SEPARATOR);
    String fileNamePart = parts[parts.length -1];
    JobIndexInfo jobIndexInfo =  FileNameIndexUtils.getIndexInfo(fileNamePart);
    return TypeConverter.fromYarn(jobIndexInfo.getJobId());
  }

  /**
   * 获取匹配配置文件的路径过滤器
   * @return 配置文件路径过滤器
   */
  public static PathFilter getConfFileFilter() {
    return CONF_FILTER;
  }
  
  /**
   * 获取匹配作业历史文件的路径过滤器
   * @return 作业历史文件路径过滤器
   */
  public static PathFilter getHistoryFileFilter() {
    return JOB_HISTORY_FILE_FILTER;
  }

  /**
   * 获取正在运行作业的历史暂存目录前缀
   * @param conf 作业配置对象
   * @param jobId 作业ID
   * @return 暂存目录前缀的字符串表示
   * @throws IOException 获取当前用户信息失败时抛出异常
   */
  public static String
      getConfiguredHistoryStagingDirPrefix(Configuration conf, String jobId)
          throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path stagingPath = MRApps.getStagingAreaDir(conf, user);
    Path path = new Path(stagingPath, jobId);
    String logDir = path.toString();
    return ensurePathInDefaultFileSystem(logDir, conf);
  }
  
  /**
   * 获取已完成作业的中间完成目录前缀
   * @param conf 配置对象
   * @return 中间完成目录前缀的字符串表示
   */
  public static String getConfiguredHistoryIntermediateDoneDirPrefix(
      Configuration conf) {
    String doneDirPrefix = conf
        .get(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR);
    if (doneDirPrefix == null) {
      doneDirPrefix = conf.get(MRJobConfig.MR_AM_STAGING_DIR,
          MRJobConfig.DEFAULT_MR_AM_STAGING_DIR)
          + "/history/done_intermediate";
    }
    return ensurePathInDefaultFileSystem(doneDirPrefix, conf);
  }

  /**
   * 获取中间完成目录下用户目录的配置权限
   * @param conf 配置对象
   * @return 用户目录的权限对象
   */
  public static FsPermission
        getConfiguredHistoryIntermediateUserDoneDirPermissions(
            Configuration conf) {
    String userDoneDirPermissions = conf.get(
        JHAdminConfig.MR_HISTORY_INTERMEDIATE_USER_DONE_DIR_PERMISSIONS);
    if (userDoneDirPermissions == null) {
      return new FsPermission(
          JHAdminConfig.DEFAULT_MR_HISTORY_INTERMEDIATE_USER_DONE_DIR_PERMISSIONS);
    }
    FsPermission permission = new FsPermission(userDoneDirPermissions);
    // 强制要求用户和用户组必须拥有全部权限，不满足则自动修正并警告
    if (permission.getUserAction() != FsAction.ALL ||
        permission.getGroupAction() != FsAction.ALL) {
      permission = new FsPermission(FsAction.ALL, FsAction.ALL,
          permission.getOtherAction(), permission.getStickyBit());
      LOG.warn("Unsupported permission configured in " +
          JHAdminConfig.MR_HISTORY_INTERMEDIATE_USER_DONE_DIR_PERMISSIONS +
          ", the user and the group permission must be 7 (rwx). " +
          "The permission was set to " + permission.toString());
    }
    return permission;
  }
  
  /**
   * 获取作业历史服务器存储已完成作业历史的根目录前缀
   * @param conf 配置对象
   * @return 已完成作业历史根目录前缀字符串
   */
  public static String getConfiguredHistoryServerDoneDirPrefix(
      Configuration conf) {
    String doneDirPrefix = conf.get(JHAdminConfig.MR_HISTORY_DONE_DIR);
    if (doneDirPrefix == null) {
      doneDirPrefix = conf.get(MRJobConfig.MR_AM_STAGING_DIR,
          MRJobConfig.DEFAULT_MR_AM_STAGING_DIR)
          + "/history/done";
    }
    return ensurePathInDefaultFileSystem(doneDirPrefix, conf);
  }

  /**
   * 获取集群默认文件系统上下文，用于统一历史目录路径格式
   * @return 默认文件系统上下文，如果仅core-default.xml配置则返回null
   */
  private static FileContext getDefaultFileContext() {
    // 如果默认文件系统仅通过core-default.xml配置，则忽略该配置
    // 避免测试场景下默认路径错误指向core-default.xml指定的文件系统
    FileContext fc = null;
    Configuration defaultConf = new Configuration();
    String[] sources;
    sources = defaultConf.getPropertySources(
        CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    if (sources != null &&
        (!Arrays.asList(sources).contains("core-default.xml") ||
        sources.length > 1)) {
      try {
        fc = FileContext.getFileContext(defaultConf);
        LOG.info("Default file system [" +
                  fc.getDefaultFileSystem().getUri() + "]");
      } catch (UnsupportedFileSystemException e) {
        LOG.error("Unable to create default file context [" +
            defaultConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) +
            "]",
            e);
      }
    }
    else {
      LOG.info("Default file system is set solely " +
          "by core-default.xml therefore -  ignoring");
    }

    return fc;
  }

  /**
   * 确保路径在集群默认文件系统上生成完全限定路径
   * 已完全限定、当前配置使用默认文件系统或无有效默认配置时直接返回原路径
   * @param sourcePath 源路径字符串
   * @param conf 作业配置对象
   * @return 处理后的完全限定路径字符串
   */
  private static String ensurePathInDefaultFileSystem(String sourcePath, Configuration conf) {
    Path path = new Path(sourcePath);
    FileContext fc = getDefaultFileContext();
    if (fc == null ||
        fc.getDefaultFileSystem().getUri().toString().equals(
            conf.getTrimmed(
                CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "")) ||
        path.toUri().getAuthority() != null ||
        path.toUri().getScheme()!= null) {
      return sourcePath;
    }

    return fc.makeQualified(path).toString();
  }

  /**
   * 获取当前用户对应的中间完成目录路径
   * @param conf 配置对象
   * @return 当前用户的中间完成目录路径字符串
   * @throws IOException 获取当前用户信息失败时抛出异常
   */
  public static String getHistoryIntermediateDoneDirForUser(Configuration conf) throws IOException {
    return new Path(getConfiguredHistoryIntermediateDoneDirPrefix(conf),
        UserGroupInformation.getCurrentUser().getShortUserName()).toString();
  }

  /**
   * 判断是否需要创建非用户属主的中间基础目录
   * 用于非安全模式单节点集群默认允许创建，无需额外配置
   * @param conf 配置对象
   * @return 需要创建返回true，否则返回false
   */
  public static boolean shouldCreateNonUserDirectory(Configuration conf) {
    // Returning true by default to allow non secure single node clusters to work
    // without any configuration change.
    return conf.getBoolean(MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR, true); 
  }

  /**
   * 获取运行中作业的历史文件路径
   * @param dir 暂存根目录
   * @param jobId 作业ID对象
   * @param attempt 作业尝试次数
   * @return 作业历史文件路径
   */
  public static Path getStagingJobHistoryFile(Path dir, JobId jobId, int attempt) {
    return getStagingJobHistoryFile(dir, TypeConverter.fromYarn(jobId).toString(), attempt);
  }
  
  /**
   * 获取运行中作业的历史文件路径
   * @param dir 暂存根目录
   * @param jobId 字符串格式作业ID
   * @param attempt 作业尝试次数
   * @return 作业历史文件路径
   */
  public static Path getStagingJobHistoryFile(Path dir, String jobId, int attempt) {
    return new Path(dir, jobId + "_" + 
        attempt + JOB_HISTORY_FILE_EXTENSION);
  }
  
  /**
   * 获取作业中间配置文件名
   * @param jobId 作业ID对象
   * @return 配置文件名字符串
   */
  public static String getIntermediateConfFileName(JobId jobId) {
    return TypeConverter.fromYarn(jobId).toString() + CONF_FILE_NAME_SUFFIX;
  }
  
  /**
   * 获取作业中间摘要文件名
   * @param jobId 作业ID对象
   * @return 摘要文件名字符串
   */
  public static String getIntermediateSummaryFileName(JobId jobId) {
    return TypeConverter.fromYarn(jobId).toString() + SUMMARY_FILE_NAME_SUFFIX;
  }
  
  /**
   * 获取运行中作业的配置文件路径
   * @param logDir 日志目录前缀
   * @param jobId 作业ID对象
   * @param attempt 作业尝试次数
   * @return 运行中作业配置文件路径
   */
  public static Path getStagingConfFile(Path logDir, JobId jobId, int attempt) {
    Path jobFilePath = null;
    if (logDir != null) {
      jobFilePath = new Path(logDir, TypeConverter.fromYarn(jobId).toString()
          + "_" + attempt + CONF_FILE_NAME_SUFFIX);
    }
    return jobFilePath;
  }
  
  /**
   * 根据作业ID生成序列号目录组件
   * @param id 作业ID对象
   * @param serialNumberFormat 序列号格式字符串
   * @return 格式化后的序列号目录组件字符串
   */
  public static String serialNumberDirectoryComponent(JobId id, String serialNumberFormat) {
    return String.format(serialNumberFormat,
        Integer.valueOf(jobSerialNumber(id))).substring(0,
        SERIAL_NUMBER_DIRECTORY_DIGITS);
  }
  
  /**
   * 从路径中提取时间戳目录组件
   * @param path 完整路径字符串
   * @return 提取得到的YYYY/MM/DD格式时间戳组件，未找到返回null
   */
  public static String getTimestampPartFromPath(String path) {
    Matcher matcher = TIMESTAMP_DIR_PATTERN.matcher(path);
    if (matcher.find()) {
      String matched = matcher.group();
      String ret = matched.intern();
      return ret;
    } else {
      return null;
    }
  }
  
  /**
   * 根据作业ID、时间戳生成历史日志子目录路径
   * @param id 作业ID对象
   * @param timestampComponent 时间戳路径组件
   * @param serialNumberFormat 序列号格式字符串
   * @return 历史日志子目录路径字符串
   */
  public static String historyLogSubdirectory(JobId id, String timestampComponent, String serialNumberFormat) {
//    String result = LOG_VERSION_STRING;
    String result = "";
    String serialNumberDirectory = serialNumberDirectoryComponent(id, serialNumberFormat);
    
    result = result 
      + timestampComponent
      + File.separator + serialNumberDirectory
      + File.separator;
    
    return result;
  }
  
  /**
   * 根据毫秒时间戳