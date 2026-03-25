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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业历史文件名索引工具类，负责生成和解析包含作业索引信息的作业历史文件名
 * 核心职责：将作业元数据编码为标准化文件名，以及从文件名反向解析出作业元数据
 * 保证向后兼容性，新字段仅追加到文件名末尾，不修改原有字段顺序
 */
public class FileNameIndexUtils {

  // 作业历史文件名分隔符，用于分隔不同元数据字段，转义后替换原分隔符避免解析错误
  static final String DELIMITER = "-";
  static final String DELIMITER_ESCAPE = "%2D";

  private static final Logger LOG =
      LoggerFactory.getLogger(FileNameIndexUtils.class);

  // 作业历史文件名字段索引定义，保持向后兼容，仅新增字段追加到末尾
  private static final int JOB_ID_INDEX = 0;
  private static final int SUBMIT_TIME_INDEX = 1;
  private static final int USER_INDEX = 2;
  private static final int JOB_NAME_INDEX = 3;
  private static final int FINISH_TIME_INDEX = 4;
  private static final int NUM_MAPS_INDEX = 5;
  private static final int NUM_REDUCES_INDEX = 6;
  private static final int JOB_STATUS_INDEX = 7;
  private static final int QUEUE_NAME_INDEX = 8;
  private static final int JOB_START_TIME_INDEX = 9;

  /**
   * 根据作业索引信息构造已完成作业的历史文件名，使用默认作业名长度限制
   * 
   * @param indexInfo 作业索引信息对象
   * @return 构造完成的作业历史文件名
   * @throws IOException 编码过程中出现IO异常时抛出
   */
  public static String getDoneFileName(JobIndexInfo indexInfo)
      throws IOException {
    return getDoneFileName(indexInfo,
        JHAdminConfig.DEFAULT_MR_HS_JOBNAME_LIMIT);
  }

  /**
   * 根据作业索引信息构造已完成作业的历史文件名，支持自定义作业名长度限制
   * 
   * @param indexInfo 作业索引信息对象
   * @param jobNameLimit 作业名最大长度限制
   * @return 构造完成的作业历史文件名
   * @throws IOException 编码过程中出现IO异常时抛出
   */
  public static String getDoneFileName(JobIndexInfo indexInfo,
      int jobNameLimit) throws IOException {
    StringBuilder sb = new StringBuilder();
    // 添加作业ID，转换格式并编码
    sb.append(encodeJobHistoryFileName(escapeDelimiters(
        TypeConverter.fromYarn(indexInfo.getJobId()).toString())));
    sb.append(DELIMITER);

    // 添加作业提交时间并编码
    sb.append(encodeJobHistoryFileName(String.valueOf(
        indexInfo.getSubmitTime())));
    sb.append(DELIMITER);

    // 添加用户名，转义分隔符后编码
    sb.append(encodeJobHistoryFileName(escapeDelimiters(
        getUserName(indexInfo))));
    sb.append(DELIMITER);

    // 添加作业名，转义分隔符编码后按长度限制裁剪
    sb.append(trimURLEncodedString(encodeJobHistoryFileName(escapeDelimiters(
        getJobName(indexInfo))), jobNameLimit));
    sb.append(DELIMITER);

    // 添加作业完成时间并编码
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getFinishTime())));
    sb.append(DELIMITER);

    // 添加Map任务数量并编码
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getNumMaps())));
    sb.append(DELIMITER);

    // 添加Reduce任务数量并编码
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getNumReduces())));
    sb.append(DELIMITER);

    // 添加作业状态并编码
    sb.append(encodeJobHistoryFileName(indexInfo.getJobStatus()));
    sb.append(DELIMITER);

    // 添加队列名称，转义分隔符后编码
    sb.append(escapeDelimiters(encodeJobHistoryFileName(
        getQueueName(indexInfo))));
    sb.append(DELIMITER);

    // 添加作业启动时间并编码
    sb.append(encodeJobHistoryFileName(
        String.valueOf(indexInfo.getJobStartTime())));

    // 添加文件扩展名
    sb.append(encodeJobHistoryFileName(
        JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION));
    return sb.toString();
  }

  /**
   * 从作业历史文件名解析出作业索引信息
   * 
   * @param jhFileName 作业历史文件名
   * @return 解析得到的作业索引信息对象
   * @throws IOException 解码过程中出现IO异常时抛出
   */
  public static JobIndexInfo getIndexInfo(String jhFileName)
      throws IOException {
    // 去除文件扩展名，保留元数据部分
    String fileName = jhFileName.substring(0,
        jhFileName.indexOf(JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION));
    JobIndexInfo indexInfo = new JobIndexInfo();

    // 按分隔符拆分所有字段
    String[] jobDetails = fileName.split(DELIMITER);

    // 解码并转换作业ID格式
    JobID oldJobId =
        JobID.forName(decodeJobHistoryFileName(jobDetails[JOB_ID_INDEX]));
    JobId jobId = TypeConverter.toYarn(oldJobId);
    indexInfo.setJobId(jobId);

    // 遇到解析错误不中断，仅记录警告保证兼容性
    try {
      // 解析提交时间
      try {
        indexInfo.setSubmitTime(Long.parseLong(
            decodeJobHistoryFileName(jobDetails[SUBMIT_TIME_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse submit time from job history file "
            + jhFileName + " : " + e);
      }

      // 解析用户名
      indexInfo.setUser(
          decodeJobHistoryFileName(jobDetails[USER_INDEX]));

      // 解析作业名
      indexInfo.setJobName(
          decodeJobHistoryFileName(jobDetails[JOB_NAME_INDEX]));

      // 解析完成时间
      try {
        indexInfo.setFinishTime(Long.parseLong(
            decodeJobHistoryFileName(jobDetails[FINISH_TIME_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse finish time from job history file "
            + jhFileName + " : " + e);
      }

      // 解析Map任务数量
      try {
        indexInfo.setNumMaps(Integer.parseInt(
            decodeJobHistoryFileName(jobDetails[NUM_MAPS_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse num maps from job history file "
            + jhFileName + " : " + e);
      }

      // 解析Reduce任务数量
      try {
        indexInfo.setNumReduces(Integer.parseInt(
            decodeJobHistoryFileName(jobDetails[NUM_REDUCES_INDEX])));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse num reduces from job history file "
            + jhFileName + " : " + e);
      }

      // 解析作业状态
      indexInfo.setJobStatus(
          decodeJobHistoryFileName(jobDetails[JOB_STATUS_INDEX]));

      // 解析队列名称
      indexInfo.setQueueName(
          decodeJobHistoryFileName(jobDetails[QUEUE_NAME_INDEX]));

      // 解析作业启动时间，兼容旧版本无此字段的文件名，使用提交时间作为默认值
      try{
        if (jobDetails.length <= JOB_START_TIME_INDEX) {
          indexInfo.setJobStartTime(indexInfo.getSubmitTime());
        } else {
          indexInfo.setJobStartTime(Long.parseLong(
              decodeJobHistoryFileName(jobDetails[JOB_START_TIME_INDEX])));
        }
      } catch (NumberFormatException e){
        LOG.warn("Unable to parse start time from job history file "
            + jhFileName + " : " + e);
      }
    } catch (IndexOutOfBoundsException e) {
      // 字段不足时仅记录警告，返回已解析的部分数据
      LOG.warn("Parsing job history file with partial data encoded into name: "
          + jhFileName);
    }

    return indexInfo;
  }

  
  /**
   * 对作业历史文件名进行URL编码，预处理转义分隔符避免编码冲突
   * 
   * @param logFileName 待编码的作业历史文件名
   * @return 编码完成的文件名
   * @throws IOException 编码不支持UTF-8时抛出
   */
  public static String encodeJobHistoryFileName(String logFileName)
  throws IOException {
    String replacementDelimiterEscape = null;

    // 临时替换已存在的转义分隔符，避免被二次编码
    if (logFileName.contains(DELIMITER_ESCAPE)) {
      replacementDelimiterEscape = nonOccursString(logFileName);

      logFileName = logFileName.replaceAll(
          DELIMITER_ESCAPE, replacementDelimiterEscape);
    }

    String encodedFileName = null;
    try {
      encodedFileName = URLEncoder.encode(logFileName, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      IOException ioe = new IOException();
      ioe.initCause(uee);
      ioe.setStackTrace(uee.getStackTrace());
      throw ioe;
    }

    // 恢复之前临时替换的转义分隔符
    if (replacementDelimiterEscape != null) {
      encodedFileName = encodedFileName.replaceAll(
          replacementDelimiterEscape, DELIMITER_ESCAPE);
    }

    return encodedFileName;
  }

  /**
   * 对URL编码的作业历史文件名进行解码
   * 
   * @param logFileName 待解码的作业历史文件名
   * @return 解码完成的文件名
   * @throws IOException 解码不支持UTF-8时抛出
   */
  public static String decodeJobHistoryFileName(String logFileName)
  throws IOException {
    String decodedFileName = null;
    try {
      decodedFileName = URLDecoder.decode(logFileName, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      IOException ioe = new IOException();
      ioe.initCause(uee);
      ioe.setStackTrace(uee.getStackTrace());
      throw ioe;
    }
    return decodedFileName;
  }

  /**
   * 生成一个文件名中不存在的临时字符串，用于占位替换
   * @param logFileName 原文件名
   * @return 原文件名中不存在的字符串
   */
  static String nonOccursString(String logFileName) {
    int adHocIndex = 0;

    String unfoundString = "q" + adHocIndex;

    while (logFileName.contains(unfoundString)) {
      unfoundString = "q" + ++adHocIndex;
    }

    return unfoundString + "q";
  }

  /**
   * 从作业索引信息获取用户名，空值返回默认占位符
   * @param indexInfo 作业索引信息
   * @return 处理后的用户名
   */
  private static String getUserName(JobIndexInfo indexInfo) {
    return getNonEmptyString(indexInfo.getUser());
  }

  /**
   * 从作业索引信息获取作业名，空值返回默认占位符
   * @param indexInfo 作业索引信息
   * @return 处理后的作业名
   */
  private static String getJobName(JobIndexInfo indexInfo) {
    return getNonEmptyString(indexInfo.getJobName());
  }

  /**
   * 从作业索引信息获取队列名，空值返回默认占位符
   * @param indexInfo 作业索引信息
   * @return 处理后的队列名
   */
  private static String getQueueName(JobIndexInfo indexInfo) {
    return getNonEmptyString(indexInfo.getQueueName());
  }

  //TODO Maybe handle default values for longs and integers here?
  
  /**
   * 确保返回非空字符串，空输入返回"NA"占位符
   * @param in 输入字符串
   * @return 处理后的非空字符串
   */
  private static String getNonEmptyString(String in) {
    if (in == null || in.length() == 0) {
      in = "NA";
    }
    return in;
  }

  /**
   * 将字符串中的分隔符转义为URL编码形式，避免拆分解析错误
   * @param escapee 待转义字符串
   * @return 转义完成的字符串
   */
  private static String escapeDelimiters(String escapee) {
    return escapee.replaceAll(DELIMITER, DELIMITER_ESCAPE);
  }

  /**
   * 按长度限制裁剪URL编码后的字符串，按UTF-8编码规则计算字节长度保证不截断
   * @param encodedString URL编码后的字符串
   * @param limitLength 最大长度限制
   * @return 裁剪完成的字符串
   */
  private static String trimURLEncodedString(
      String encodedString, int limitLength) {
    assert(limitLength >= 0) : "limitLength should be positive integer";

    if (encodedString.length() <= limitLength) {
      return encodedString;
    }

    int index = 0;
    int increase = 0;
    byte[] strBytes = encodedString.getBytes(UTF_8);

    // 根据RFC3629 UTF-8规范计算每个字符占用的字节长度，避免截断URL编码字符
    while (true) {
      byte b = strBytes[index];
      if (b == '%') {
        // 解析URL编码的十六进制字节值
        byte minuend1 = strBytes[index + 1];
        byte subtrahend1 = (byte)(Character.isDigit(
            minuend1) ? '0' : 'A' - 10);
        byte minuend2 = strBytes[index + 2];
        byte subtrahend2 = (byte)(Character.isDigit(
            minuend2) ? '0' : 'A' - 10);
        int initialHex =
            ((Character.toUpperCase(minuend1) - subtrahend1) << 4) +
            (Character.toUpperCase(minuend2) - subtrahend2);

        // 根据UTF-8编码范围计算当前字符占用字节数，URL编码每个字节占3个字符
        if (0x00 <= initialHex && initialHex <= 0x7F) {
          // 1字节UTF-8字符
          increase = 3;
        } else if (0xC2 <= initialHex && initialHex <= 0xDF) {
          // 2字节UTF-8字符
          increase = 6;
        } else if (0xE0 <= initialHex && initialHex <= 0xEF) {
          // 3字节UTF-8字符
          increase = 9;
        } else {
          // 4字节UTF-8字符
          increase = 12;
        }
      } else {
        // 非URL编码字符占1字节
        increase = 1;
      }
      // 超过长度限制则停止，否则累加索引
      if (index + increase > limitLength) {
        break;
      } else {
        index += increase;
      }
    }

    return encodedString.substring(0, index);
  }
}