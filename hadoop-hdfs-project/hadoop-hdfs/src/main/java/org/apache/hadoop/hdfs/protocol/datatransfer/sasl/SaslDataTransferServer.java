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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY;
import static org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil.*;

import org.apache.hadoop.classification.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
arrya
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.security.CustomizedCallbackHandler;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS数据传输协议服务端SASL协商处理类，为DataNode处理入站连接的SASL认证
 * 支持两种SASL协商模式：
 * 1. 通用模式：支持任意QOP（保护质量）级别
 * 2. 加密专用模式：强制使用隐私保护级别，基于高强度加密密钥
 */
@InterfaceAudience.Private
public class SaslDataTransferServer {

  private static final Logger LOG = LoggerFactory.getLogger(
    SaslDataTransferServer.class);

  private final BlockPoolTokenSecretManager blockPoolTokenSecretManager;
  private final DNConf dnConf;

  // Store the most recent successfully negotiated QOP,
  // for testing purpose only
  private String negotiatedQOP;

  /**
   * 构造服务端SASL协商处理器
   * @param dnConf DataNode配置对象
   * @param blockPoolTokenSecretManager 块池密钥管理器，用于验证块访问令牌和加密密钥
   */
  public SaslDataTransferServer(DNConf dnConf,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager) {
    this.blockPoolTokenSecretManager = blockPoolTokenSecretManager;
    this.dnConf = dnConf;
  }

  /**
   * 接收并处理客户端发起的SASL协商，根据配置选择对应协商模式
   * @param peer 连接对端对象
   * @param underlyingOut 底层输出流
   * @param underlyingIn 底层输入流
   * @param xferPort DataNode数据传输端口
   * @param datanodeId 当前接收连接的DataNodeID
   * @return SASL协商包装后的IO流对
   * @throws IOException 协商过程中发生任何错误则抛出
   */
  public IOStreamPair receive(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn, int xferPort, DatanodeID datanodeId)
      throws IOException {
    if (dnConf.getEncryptDataTransfer()) {
      LOG.debug(
        "SASL server doing encrypted handshake for peer = {}, datanodeId = {}",
        peer, datanodeId);
      return getEncryptedStreams(peer, underlyingOut, underlyingIn);
    } else if (!UserGroupInformation.isSecurityEnabled()) {
      LOG.debug(
        "SASL server skipping handshake in unsecured configuration for "
        + "peer = {}, datanodeId = {}", peer, datanodeId);
      return new IOStreamPair(underlyingIn, underlyingOut);
    } else if (SecurityUtil.isPrivilegedPort(xferPort)) {
      LOG.debug(
        "SASL server skipping handshake in secured configuration for "
        + "peer = {}, datanodeId = {}", peer, datanodeId);
      return new IOStreamPair(underlyingIn, underlyingOut);
    } else if (dnConf.getSaslPropsResolver() != null) {
      LOG.debug(
        "SASL server doing general handshake for peer = {}, datanodeId = {}",
        peer, datanodeId);
      return getSaslStreams(peer, underlyingOut, underlyingIn);
    } else if (dnConf.getIgnoreSecurePortsForTesting()) {
      // It's a secured cluster using non-privileged ports, but no SASL.  The
      // only way this can happen is if the DataNode has
      // ignore.secure.ports.for.testing configured, so this is a rare edge case.
      LOG.debug(
        "SASL server skipping handshake in secured configuration with no SASL "
        + "protection configured for peer = {}, datanodeId = {}",
        peer, datanodeId);
      return new IOStreamPair(underlyingIn, underlyingOut);
    } else {
      // The error message here intentionally does not mention
      // ignore.secure.ports.for.testing.  That's intended for dev use only.
      // This code path is not expected to execute ever, because DataNode startup
      // checks for invalid configuration and aborts.
      throw new IOException(String.format("Cannot create a secured " +
        "connection if DataNode listens on unprivileged port (%d) and no " +
        "protection is defined in configuration property %s.",
        datanodeId.getXferPort(), DFS_DATA_TRANSFER_PROTECTION_KEY));
    }
  }

  /**
   * 执行专用加密模式SASL握手协商
   * @param peer 连接对端对象
   * @param underlyingOut 底层输出流
   * @param underlyingIn 底层输入流
   * @return 协商包装后的IO流对
   * @throws IOException 协商错误抛出异常
   */
  private IOStreamPair getEncryptedStreams(Peer peer,
      OutputStream underlyingOut, InputStream underlyingIn) throws IOException {
    if (peer.hasSecureChannel() ||
        dnConf.getTrustedChannelResolver().isTrusted(getPeerAddress(peer))) {
      return new IOStreamPair(underlyingIn, underlyingOut);
    }

    Map<String, String> saslProps = createSaslPropertiesForEncryption(
      dnConf.getEncryptionAlgorithm());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Server using encryption algorithm " +
        dnConf.getEncryptionAlgorithm());
    }

    final CallbackHandler callbackHandler = new SaslServerCallbackHandler(dnConf.getConf(),
      new PasswordFunction() {
        @Override
        public char[] apply(String userName) throws IOException {
          return encryptionKeyToPassword(getEncryptionKeyFromUserName(userName));
        }
      });
    return doSaslHandshake(peer, underlyingOut, underlyingIn, saslProps,
        callbackHandler);
  }

  /**
   * SASL密码获取函数接口，用于参数化不同协商模式的密码获取逻辑
   */
  interface PasswordFunction {

    /**
     * 根据给定用户名获取对应SASL密码
     * @param userName SASL协商用户名
     * @return SASL密码字符数组
     * @throws IOException 获取密码过程发生错误则抛出
     */
    char[] apply(String userName) throws IOException;
  }

  /**
   * SASL服务端回调处理器，处理SASL库回调请求，完成用户名密码获取与授权
   */
  static final class SaslServerCallbackHandler
      implements CallbackHandler {
    private final PasswordFunction passwordFunction;
    private final CustomizedCallbackHandler customizedCallbackHandler;

    /**
     * 构造SASL服务端回调处理器
     * @param conf Hadoop配置对象
     * @param passwordFunction 密码获取函数
     */
    SaslServerCallbackHandler(Configuration conf, PasswordFunction passwordFunction) {
      this.passwordFunction = passwordFunction;
      this.customizedCallbackHandler = CustomizedCallbackHandler.get(
          HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY, conf);
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      List<Callback> unknownCallbacks = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          if (unknownCallbacks == null) {
            unknownCallbacks = new ArrayList<>();
          }
          unknownCallbacks.add(callback);
        }
      }

      if (pc != null) {
        pc.setPassword(passwordFunction.apply(nc.getDefaultName()));
      }

      if (ac != null) {
        ac.setAuthorized(true);
        ac.setAuthorizedID(ac.getAuthorizationID());
      }

      if (unknownCallbacks != null) {
        final String name = nc != null ? nc.getDefaultName() : null;
        final char[] password = name != null ? passwordFunction.apply(name) : null;
        customizedCallbackHandler.handleCallbacks(unknownCallbacks, name, password);
      }
    }
  }

  /**
   * 从加密握手用户名中解析出加密密钥
   * @param userName 包含keyId、块池ID、nonce的用户名
   * @return 解密得到的数据加密密钥
   * @throws IOException 解析或密钥检索错误抛出异常
   */
  private byte[] getEncryptionKeyFromUserName(String userName)
      throws IOException {
    String[] nameComponents = userName.split(NAME_DELIMITER);
    if (nameComponents.length != 3) {
      throw new IOException("Provided name '" + userName + "' has " +
          nameComponents.length + " components instead of the expected 3.");
    }
    int keyId = Integer.parseInt(nameComponents[0]);
    String blockPoolId = nameComponents[1];
    byte[] nonce = Base64.decodeBase64(nameComponents[2]);
    return blockPoolTokenSecretManager.retrieveDataEncryptionKey(keyId,
        blockPoolId, nonce);
  }

  /**
   * 执行通用模式SASL握手协商
   * @param peer 连接对端对象
   * @param underlyingOut 底层输出流
   * @param underlyingIn 底层输入流
   * @return 协商包装后的IO流对
   * @throws IOException 协商错误抛出异常
   */
  private IOStreamPair getSaslStreams(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn) throws IOException {
    if (peer.hasSecureChannel() ||
        dnConf.getTrustedChannelResolver().isTrusted(getPeerAddress(peer))) {
      return new IOStreamPair(underlyingIn, underlyingOut);
    }

    SaslPropertiesResolver saslPropsResolver = dnConf.getSaslPropsResolver();
    Map<String, String> saslProps = saslPropsResolver.getServerProperties(
      getPeerAddress(peer));

    final CallbackHandler callbackHandler = new SaslServerCallbackHandler(dnConf.getConf(),
      new PasswordFunction() {
        @Override
        public char[] apply(String userName) throws IOException {
          return buildServerPassword(userName);
        }
    });
    return doSaslHandshake(peer, underlyingOut, underlyingIn, saslProps,
        callbackHandler);
  }

  /**
   * 为通用模式握手计算服务端期望密码，密码来自块访问令牌的密钥
   * @param userName 包含序列化块访问令牌ID的SASL用户名
   * @return 服务端期望的SASL密码
   * @throws IOException 反序列化或密码检索错误抛出异常
   */    
  private char[] buildServerPassword(String userName) throws IOException {
    BlockTokenIdentifier identifier = deserializeIdentifier(userName);
    byte[] tokenPassword = blockPoolTokenSecretManager.retrievePassword(
      identifier);
    return (new String(Base64.encodeBase64(tokenPassword, false),
      StandardCharsets.UTF_8)).toCharArray();
  }

  /**
   * 反序列化base64编码的块访问令牌ID
   * @param str base64编码的块令牌ID字符串
   * @return 反序列化得到的BlockTokenIdentifier对象
   * @throws IOException 反序列化IO错误抛出异常
   */
  private BlockTokenIdentifier deserializeIdentifier(String str)
      throws IOException {
    BlockTokenIdentifier identifier = new BlockTokenIdentifier();
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(
      Base64.decodeBase64(str))));
    return identifier;
  }

  @VisibleForTesting
  public String getNegotiatedQOP() {
    return negotiatedQOP;
  }

  /**
   * 执行服务端SASL握手核心流程
   * @param peer 连接对端对象
   * @param underlyingOut 底层输出流
   * @param underlyingIn 底层输入流
   * @param saslProps SASL协商属性
   * @param callbackHandler SASL回调处理器
   * @return 协商包装后的IO流对
   * @throws IOException 握手过程任何错误抛出异常
   */
  private IOStreamPair doSaslHandshake(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn, Map<String, String> saslProps,
      CallbackHandler callbackHandler) throws IOException {

    DataInputStream in = new DataInputStream(underlyingIn);
    DataOutputStream out = new DataOutputStream(underlyingOut);

    // 读取并验证协商魔数
    int magicNumber = in.readInt();
    if (magicNumber != SASL_TRANSFER_MAGIC_NUMBER) {
      throw new InvalidMagicNumberException(magicNumber, 
          dnConf.getEncryptDataTransfer());
    }
    try {
      // step 1: 读取客户端初始消息
      SaslMessageWithHandshake message = readSaslMessageWithHandshakeSecret(in);
      byte[] secret = message.getSecret();
      String bpid = message.getBpid();
      // 创建可修改的SASL属性副本
      Map<String, String> dynamicSaslProps = new TreeMap<>(saslProps);
      if (secret != null || bpid != null) {
        // 一致性检查，secret和bpid必须同时存在
        assert(secret != null && bpid != null);
        // 客户端动态指定QOP，更新属性
        String qop = new String(secret, StandardCharsets.UTF_8);
        saslProps.put(Sasl.QOP, qop);
        dynamicSaslProps.put(Sasl.QOP, qop);
      }
      // 创建服务端SASL协商参与者
      SaslParticipant sasl = SaslParticipant.createServerSaslParticipant(
          dynamicSaslProps, callbackHandler);

      byte[] remoteResponse = message.getPayload();
      // 处理客户端挑战/响应，生成本地响应
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      // 发送响应给客户端
      sendSaslMessage(out, localResponse);

      // step 2: 读取客户端的密码套件协商消息
      List<CipherOption> cipherOptions = Lists.newArrayList();
      remoteResponse = readSaslMessageAndNegotiationCipherOptions(
          in, cipherOptions);
      localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);

      // 检查SASL握手是否完成
      checkSaslComplete(sasl, dynamicSaslProps);

      CipherOption cipherOption = null;
      // 保存协商结果QOP供测试使用
      negotiated