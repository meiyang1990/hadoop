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
package org.apache.hadoop.hdfs.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.StringUtils;


/**
 * 处理Unix md5sum工具格式的MD5校验文件工具类，提供MD5校验文件的读写、验证、重命名等功能
 */
public abstract class MD5FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(
      MD5FileUtils.class);

  public static final String MD5_SUFFIX = ".md5";
  private static final Pattern LINE_REGEX =
    Pattern.compile("([0-9a-f]{32}) [ \\*](.+)");
  
  /**
   * 验证给定数据文件对应的存储MD5校验值是否与预期一致
   * @param dataFile 待验证的数据文件
   * @param expectedMD5 预期正确的MD5校验值
   * @throws IOException 当校验不匹配或读取校验文件失败时抛出异常
   */
  public static void verifySavedMD5(File dataFile, MD5Hash expectedMD5)
      throws IOException {
    MD5Hash storedHash = readStoredMd5ForFile(dataFile);
    // 检查MD5值是否匹配
    if (!expectedMD5.equals(storedHash)) {
      throw new IOException(
          "File " + dataFile + " did not match stored MD5 checksum " +
          " (stored: " + storedHash + ", computed: " + expectedMD5);
    }
  }
  
  /**
   * 读取MD5校验文件，将内容匹配到正则提取组
   * @param md5File MD5校验文件
   * @return 匹配后的Matcher对象，group(1)为MD5字符串，group(2)为数据文件路径
   * @throws IOException 读取文件或内容格式不匹配时抛出异常
   */
  private static Matcher readStoredMd5(File md5File) throws IOException {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(
            Files.newInputStream(md5File.toPath()), StandardCharsets.UTF_8));
    String md5Line;
    try {
      // 读取MD5文件第一行
      md5Line = reader.readLine();
      if (md5Line == null) { md5Line = ""; }
      md5Line = md5Line.trim();
    } catch (IOException ioe) {
      throw new IOException("Error reading md5 file at " + md5File, ioe);
    } finally {
      // 关闭流并记录日志
      IOUtils.cleanupWithLogger(LOG, reader);
    }
    
    Matcher matcher = LINE_REGEX.matcher(md5Line);
    if (!matcher.matches()) {
      throw new IOException("Invalid MD5 file " + md5File + ": the content \""
          + md5Line + "\" does not match the expected pattern.");
    }
    return matcher;
  }

  /**
   * 读取指定数据文件对应的同目录.md5文件中的MD5校验值
   * @param dataFile 目标数据文件
   * @return 存储在dataFile.md5中的MD5校验值，若校验文件不存在返回null
   * @throws IOException 读取文件或校验失败时抛出异常
   */
  public static MD5Hash readStoredMd5ForFile(File dataFile) throws IOException {
    final File md5File = getDigestFileForFile(dataFile);
    if (!md5File.exists()) {
      return null;
    }

    final Matcher matcher = readStoredMd5(md5File);
    String storedHash = matcher.group(1);
    File referencedFile = new File(matcher.group(2));

    // 合理性检查：确保MD5文件中记录的文件名与实际数据文件名一致
    if (!referencedFile.getName().equals(dataFile.getName())) {
      throw new IOException(
          "MD5 file at " + md5File + " references file named " +
          referencedFile.getName() + " but we expected it to reference " +
          dataFile);
    }
    return new MD5Hash(storedHash);
  }
  
  /**
   * 计算指定文件内容的MD5校验值
   * @param dataFile 待计算的目标文件
   * @return 计算得到的MD5校验值对象
   * @throws IOException 读取文件失败时抛出异常
   */
  public static MD5Hash computeMd5ForFile(File dataFile) throws IOException {
    InputStream in = Files.newInputStream(dataFile.toPath());
    try {
      MessageDigest digester = MD5Hash.getDigester();
      DigestInputStream dis = new DigestInputStream(in, digester);
      // 读取整个文件更新摘要
      IOUtils.copyBytes(dis, new IOUtils.NullOutputStream(), 128*1024);
      
      return new MD5Hash(digester.digest());
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * 为指定数据文件保存MD5校验值到同目录.md5文件中，遵循md5sum格式
   * @param dataFile 原始数据文件
   * @param digest 计算得到的MD5摘要
   * @throws IOException 写入文件失败时抛出异常
   */
  public static void saveMD5File(File dataFile, MD5Hash digest)
      throws IOException {
    final String digestString = StringUtils.byteToHexString(digest.getDigest());
    saveMD5File(dataFile, digestString);
  }

  /**
   * 实际执行写入MD5校验文件的操作，使用原子输出流保证写入安全
   * @param dataFile 原始数据文件
   * @param digestString 十六进制格式的MD5字符串
   * @throws IOException 写入文件失败时抛出异常
   */
  private static void saveMD5File(File dataFile, String digestString)
      throws IOException {
    File md5File = getDigestFileForFile(dataFile);
    String md5Line = digestString + " *" + dataFile.getName() + "\n";

    // 使用原子输出流，避免写入中途失败导致文件损坏
    AtomicFileOutputStream afos = new AtomicFileOutputStream(md5File);
    afos.write(md5Line.getBytes(StandardCharsets.UTF_8));
    afos.close();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Saved MD5 " + digestString + " to " + md5File);
    }
  }

  /**
   * 将原数据文件对应的MD5校验文件重命名为对应新数据文件的名称，更新内容后保存，删除原校验文件
   * @param oldDataFile 原数据文件
   * @param newDataFile 新数据文件
   * @throws IOException 读取、写入或删除文件失败时抛出异常
   */
  public static void renameMD5File(File oldDataFile, File newDataFile)
      throws IOException {
    final File fromFile = getDigestFileForFile(oldDataFile);
    if (!fromFile.exists()) {
      throw new FileNotFoundException(fromFile + " does not exist.");
    }

    // 读取原MD5值，保存到对应新文件的MD5文件中
    final String digestString = readStoredMd5(fromFile).group(1);
    saveMD5File(newDataFile, digestString);

    // 删除原MD5校验文件，删除失败记录警告日志
    if (!fromFile.delete()) {
      LOG.warn("deleting  " + fromFile.getAbsolutePath() + " FAILED");
    }
  }

  /**
   * 获取给定数据文件对应的MD5校验文件对象，路径为同目录+原文件名+.md5后缀
   * @param file 原始数据文件
   * @return 对应的MD5校验文件对象
   */
  public static File getDigestFileForFile(File file) {
    return new File(file.getParentFile(), file.getName() + MD5_SUFFIX);
  }
}