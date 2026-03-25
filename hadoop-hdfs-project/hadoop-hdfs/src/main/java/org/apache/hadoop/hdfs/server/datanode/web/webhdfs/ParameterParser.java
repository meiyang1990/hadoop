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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.CreateFlagParam;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.hdfs.web.resources.NoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.UnmaskedPermissionParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX_LENGTH;

/**
 * WebHDFS REST API DataNode端HTTP请求参数解析器，负责从请求URL中提取并转换各类WebHDFS操作参数
 */
class ParameterParser {
  private final Configuration conf;
  private final String path;
  private final Map<String, List<String>> params;

  /**
   * 构造参数解析器，从Netty查询字符串解码器中提取请求信息
   * @param decoder Netty HTTP查询字符串解码器
   * @param conf Hadoop配置对象
   */
  ParameterParser(QueryStringDecoder decoder, Configuration conf) {
    this.path = decoder.path().substring(WEBHDFS_PREFIX_LENGTH);
    this.params = decoder.parameters();
    this.conf = conf;
  }

  /**
   * 获取请求目标文件/目录的路径
   * @return 剥离了WebHDFS前缀后的路径
   */
  String path() { return path; }

  /**
   * 获取当前请求的HTTP操作类型
   * @return WebHDFS操作名称
   */
  String op() {
    return param(HttpOpParam.NAME);
  }

  /**
   * 获取读操作起始偏移量
   * @return 偏移量数值
   */
  long offset() {
    return new OffsetParam(param(OffsetParam.NAME)).getOffset();
  }

  /**
   * 获取读操作请求读取长度
   * @return 长度数值
   */
  long length() {
    return new LengthParam(param(LengthParam.NAME)).getLength();
  }

  /**
   * 获取目标NameNode标识
   * @return NameNode地址/ID字符串
   */
  String namenodeId() {
    return new NamenodeAddressParam(param(NamenodeAddressParam.NAME))
      .getValue();
  }

  /**
   * 获取代理请求用户（doAs）
   * @return 代理用户名
   */
  String doAsUser() {
    return new DoAsParam(param(DoAsParam.NAME)).getValue();
  }

  /**
   * 获取操作用户名
   * @return 用户名
   */
  String userName() {
    return new UserParam(param(UserParam.NAME)).getValue();
  }

  /**
   * 获取IO缓冲区大小
   * @return 缓冲区大小，使用配置默认值若未指定
   */
  int bufferSize() {
    return new BufferSizeParam(param(BufferSizeParam.NAME)).getValue(conf);
  }

  /**
   * 获取文件块大小
   * @return 文件块大小，使用配置默认值若未指定
   */
  long blockSize() {
    return new BlockSizeParam(param(BlockSizeParam.NAME)).getValue(conf);
  }

  /**
   * 获取文件副本数
   * @return 副本数，使用配置默认值若未指定
   */
  short replication() {
    return new ReplicationParam(param(ReplicationParam.NAME)).getValue(conf);
  }

  /**
   * 获取文件权限
   * @return 文件权限对象
   */
  FsPermission permission() {
    return new PermissionParam(param(PermissionParam.NAME)).
        getFileFsPermission();
  }

  /**
   * 获取未掩码处理的文件权限
   * @return 未掩码权限对象，若未指定则返回null
   */
  FsPermission unmaskedPermission() {
    String value = param(UnmaskedPermissionParam.NAME);
    return value == null ? null :
        new UnmaskedPermissionParam(value).getFileFsPermission();
  }

  /**
   * 获取文件覆盖标志
   * @return 是否覆盖已有文件
   */
  boolean overwrite() {
    return new OverwriteParam(param(OverwriteParam.NAME)).getValue();
  }

  /**
   * 获取是否禁止重定向标志
   * @return true表示不重定向，false表示允许重定向
   */
  boolean noredirect() {
    return new NoRedirectParam(param(NoRedirectParam.NAME)).getValue();
  }

  /**
   * 从请求参数解析并构造委托令牌，处理HA逻辑名称地址场景
   * @return 构造完成的委托令牌，若参数不存在则返回null
   * @throws IOException 令牌解码失败时抛出IO异常
   */
  Token<DelegationTokenIdentifier> delegationToken() throws IOException {
    String delegation = param(DelegationParam.NAME);
    if (delegation == null) {
      return null;
    }
    final Token<DelegationTokenIdentifier> token = new
      Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(delegation);
    URI nnUri = URI.create(HDFS_URI_SCHEME + "://" + namenodeId());
    boolean isLogical = HAUtilClient.isLogicalUri(conf, nnUri);
    if (isLogical) {
      // HA场景构造逻辑URI对应的令牌服务地址
      token.setService(
          HAUtilClient.buildTokenServiceForLogicalUri(nnUri, HDFS_URI_SCHEME));
    } else {
      // 非HA场景构造普通NameNode URI对应的令牌服务地址
      token.setService(SecurityUtil.buildTokenService(nnUri));
    }
    return token;
  }

  /**
   * 获取是否自动创建父目录标志
   * @return true表示自动创建不存在的父目录
   */
  public boolean createParent() {
    return new CreateParentParam(param(CreateParentParam.NAME)).getValue();
  }

  /**
   * 获取文件创建标志集合
   * @return 创建标志枚举集合
   */
  public EnumSet<CreateFlag> createFlag() {
    String cf = "";
    if (param(CreateFlagParam.NAME) != null) {
      // 对创建标志参数进行二次URL解码，处理URL编码问题
      QueryStringDecoder decoder = new QueryStringDecoder(
          param(CreateFlagParam.NAME),
          StandardCharsets.UTF_8);
      cf = decoder.path();
    }
    return new CreateFlagParam(cf).getValue();
  }

  /**
   * 获取Hadoop配置对象
   * @return 配置对象
   */
  Configuration conf() {
    return conf;
  }

  /**
   * 根据参数名获取首个参数值
   * @param key 参数名
   * @return 第一个参数值，若不存在则返回null
   */
  private String param(String key) {
    List<String> p = params.get(key);
    return p == null ? null : p.get(0);
  }

  /**
   * 辅助方法，解码单个十六进制字符（4位二进制）
   * @param c 要解码的ASCII十六进制字符，必须在[0-9a-fA-F]范围内
   * @return 解码后的十六进制数值，字符非法则返回Character.MAX_VALUE
   */
  private static char decodeHexNibble(final char c) {
    if ('0' <= c && c <= '9') {
      return (char) (c - '0');
    } else if ('a' <= c && c <= 'f') {
      return (char) (c - 'a' + 10);
    } else if ('A' <= c && c <= 'F') {
      return (char) (c - 'A' + 10);
    } else {
      return Character.MAX_VALUE;
    }
  }
}