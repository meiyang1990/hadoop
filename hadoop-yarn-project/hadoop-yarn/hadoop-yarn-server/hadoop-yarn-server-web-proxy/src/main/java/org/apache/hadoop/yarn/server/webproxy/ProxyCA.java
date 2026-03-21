// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.webproxy;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.UUID;

/**
 * YARN Web Proxy证书颁发机构，负责生成和验证ApplicationMaster与RM Proxy之间
 * HTTPS通信使用的专用HTTPS证书。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProxyCA {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyCA.class);

  // 根CA证书
  private X509Certificate caCert;
  // 根CA密钥对
  private KeyPair caKeyPair;
  // 信任存储，信任当前根CA证书
  private KeyStore childTrustStore;
  // 安全随机数生成器
  private final Random srand;
  // 系统默认X509信任管理器
  private X509TrustManager defaultTrustManager;
  // X509密钥管理器
  private X509KeyManager x509KeyManager;
  // 主机名验证器
  private HostnameVerifier hostnameVerifier;
  // 签名算法标识：SHA512withRSA
  private static final AlgorithmIdentifier SIG_ALG_ID =
      new DefaultSignatureAlgorithmIdentifierFinder().find("SHA512WITHRSA");

  /**
   * 构造ProxyCA实例，初始化安全随机数并注册BouncyCastle加密提供器
   */
  public ProxyCA() {
    srand = new SecureRandom();

    // BouncyCastle提供器只需注册一次
    Security.addProvider(new BouncyCastleProvider());
  }

  /**
   * 初始化ProxyCA，自动生成根CA证书和密钥对
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  public void init() throws GeneralSecurityException, IOException {
    createCACertAndKeyPair();
    initInternal();
  }

  /**
   * 使用外部提供的根CA证书和私钥初始化ProxyCA，验证失败则自动重新生成
   * @param caCert 根CA证书
   * @param caPrivateKey 根CA私钥
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  public void init(X509Certificate caCert, PrivateKey caPrivateKey)
      throws GeneralSecurityException, IOException {
    if (caCert == null || caPrivateKey == null
        || !verifyCertAndKeys(caCert, caPrivateKey)) {
      LOG.warn("Could not verify Certificate, Public Key, and Private Key: " +
          "regenerating");
      createCACertAndKeyPair();
    } else {
      this.caCert = caCert;
      this.caKeyPair = new KeyPair(caCert.getPublicKey(), caPrivateKey);
    }
    initInternal();
  }

  /**
   * 内部初始化：加载默认信任管理器、创建密钥管理器、主机名验证器和信任存储
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  private void initInternal() throws GeneralSecurityException, IOException {
    defaultTrustManager = null;
    TrustManagerFactory factory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
    // 使用默认信任管理器初始化，加载系统信任的CA证书
    factory.init((KeyStore) null);
    // 查找默认X509信任管理器
    for (TrustManager manager : factory.getTrustManagers()) {
      if (manager instanceof X509TrustManager) {
        defaultTrustManager = (X509TrustManager) manager;
        break;
      }
    }
    if (defaultTrustManager == null) {
      throw new YarnRuntimeException(
          "Could not find default X509 Trust Manager");
    }

    this.x509KeyManager = createKeyManager();
    this.hostnameVerifier = createHostnameVerifier();
    this.childTrustStore = createTrustStore("client", caCert);
  }

  /**
   * 使用BouncyCastle生成X509证书
   * @param isCa 是否为CA证书
   * @param issuerStr 颁发者DN
   * @param subjectStr 主体DN
   * @param from 证书生效时间
   * @param to 证书过期时间
   * @param publicKey 证书公钥
   * @param privateKey 签名私钥
   * @return 生成的X509证书
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  private X509Certificate createCert(boolean isCa, String issuerStr,
      String subjectStr, Date from, Date to, PublicKey publicKey,
      PrivateKey privateKey) throws GeneralSecurityException, IOException {
    X500Name issuer = new X500Name(issuerStr);
    X500Name subject = new X500Name(subjectStr);
    SubjectPublicKeyInfo subPubKeyInfo =
        SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
    // 创建证书生成器，使用64位随机序列号
    X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
        issuer, new BigInteger(64, srand), from, to, subject, subPubKeyInfo);
    AlgorithmIdentifier digAlgId =
        new DefaultDigestAlgorithmIdentifierFinder().find(SIG_ALG_ID);
    ContentSigner contentSigner;
    try {
      // 构建RSA内容签名器
      contentSigner = new BcRSAContentSignerBuilder(SIG_ALG_ID, digAlgId)
          .build(PrivateKeyFactory.createKey(privateKey.getEncoded()));
    } catch (OperatorCreationException oce) {
      throw new GeneralSecurityException(oce);
    }
    if (isCa) {
      // BasicConstraints(0)表示这是CA证书，且路径长度为0，
      // 意味着子证书不能再签发孙证书，限制证书层级只有两级（CA-应用证书）
      certBuilder.addExtension(Extension.basicConstraints, true,
          new BasicConstraints(0));
    } else {
      // BasicConstraints(false)表示这不是CA证书，不能签发其他证书
      certBuilder.addExtension(Extension.basicConstraints, true,
          new BasicConstraints(false));
      // 添加CA证书的颁发者密钥标识
      certBuilder.addExtension(Extension.authorityKeyIdentifier, false,
          new JcaX509ExtensionUtils().createAuthorityKeyIdentifier(caCert));
    }
    // 签名生成证书
    X509CertificateHolder certHolder = certBuilder.build(contentSigner);
    X509Certificate cert = new JcaX509CertificateConverter().setProvider("BC")
        .getCertificate(certHolder);
    LOG.info("Created Certificate for {}", subject);
    return cert;
  }

  /**
   * 生成自签名根CA证书和密钥对
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  private void createCACertAndKeyPair()
      throws GeneralSecurityException, IOException {
    Date from = new Date();
    // 证书过期时间固定到2037年底
    Date to = new GregorianCalendar(2037, Calendar.DECEMBER, 31).getTime();
    // 生成2048位RSA密钥对
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    caKeyPair = keyGen.genKeyPair();
    // 生成随机主体名称
    String subject = "OU=YARN-" + UUID.randomUUID();
    // 生成自签名CA证书
    caCert = createCert(true, subject, subject, from, to,
        caKeyPair.getPublic(), caKeyPair.getPrivate());
    LOG.debug("CA Certificate: \n{}", caCert);
  }

  /**
   * 为指定应用生成专属密钥库，包含应用证书和私钥
   * @param appId 应用ID
   * @param ksPassword 密钥库密码
   * @return 密钥库字节数组
   * @throws Exception 生成过程中可能的异常
   */
  public byte[] createChildKeyStore(ApplicationId appId, String ksPassword)
      throws Exception {
    // 不对应用证书设置过期时间，应用停止后证书自然失效，即使被误用也无法通过应用ID校验
    Date from = new Date();
    Date to = from;
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    KeyPair keyPair = keyGen.genKeyPair();
    String issuer = caCert.getSubjectX500Principal().getName();
    // 主体名称使用应用ID，方便后续校验
    String subject = "CN=" + appId;
    // 由根CA签发应用证书
    X509Certificate cert = createCert(false, issuer, subject, from, to,
        keyPair.getPublic(), caKeyPair.getPrivate());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Certificate for {}: \n{}", appId, cert);
    }

    KeyStore keyStore = createChildKeyStore(ksPassword, "server",
        keyPair.getPrivate(), cert);
    return keyStoreToBytes(keyStore, ksPassword);
  }

  /**
   * 获取信任当前根CA的信任存储字节数组
   * @param password 信任存储密码
   * @return 信任存储字节数组
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  public byte[] getChildTrustStore(String password)
      throws GeneralSecurityException, IOException {
    return keyStoreToBytes(childTrustStore, password);
  }

  /**
   * 创建空的JKS密钥库
   * @return 空密钥库
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  private KeyStore createEmptyKeyStore()
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // 初始化空密钥库
    return ks;
  }

  /**
   * 创建包含应用私钥和证书的子密钥库
   * @param password 密钥库密码
   * @param alias 别名
   * @param privateKey 应用私钥
   * @param cert 应用证书
   * @return 创建好的子密钥库
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  private KeyStore createChildKeyStore(String password, String alias,
      Key privateKey, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(),
        new Certificate[]{cert, caCert});
    return ks;
  }

  /**
   * 生成随机16位密钥库密码
   * @return 随机密码字符串
   */
  public String generateKeyStorePassword() {
    return RandomStringUtils.random(16, 0, 0, true, true, null, srand);
  }

  /**
   * 将密钥库导出为字节数组
   * @param ks 密钥库
   * @param password 密钥库密码
   * @return 密钥库字节数组
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  private byte[] keyStoreToBytes(KeyStore ks, String password)
      throws GeneralSecurityException, IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      ks.store(out, password.toCharArray());
      return out.toByteArray();
    }
  }

  /**
   * 创建信任存储，将指定证书加入信任列表
   * @param alias 证书别名
   * @param cert 要信任的证书
   * @return 创建好的信任存储
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  private KeyStore createTrustStore(String alias, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setCertificateEntry(alias, cert);
    return ks;
  }

  /**
   * 为指定应用创建定制SSL上下文，包含自定义信任管理器验证应用证书
   * @param appId 应用ID
   * @return 定制SSL上下文
   * @throws GeneralSecurityException 安全相关异常
   */
  public SSLContext createSSLContext(ApplicationId appId)
      throws GeneralSecurityException {
    // 由于SSL规范仅会使用第一个X509TrustManager，因此需要我们自定义信任管理器
    // 同时处理默认信任和ProxyCA签发的应用证书信任
    TrustManager[] trustManagers = new TrustManager[] {
        createTrustManager(appId)};
    KeyManager[] keyManagers = new KeyManager[]{x509KeyManager};

    SSLContext sc = SSLContext.getInstance("SSL");
    sc.init(keyManagers, trustManagers, new SecureRandom());
    return sc;
  }

  /**
   * 创建自定义信任管理器，优先验证ProxyCA签发的应用证书，验证失败回退到默认信任管理器
   * @param appId 期望的应用ID
   * @return 自定义X509信任管理器
   */
  @VisibleForTesting
  X509TrustManager createTrustManager(ApplicationId appId) {
    return new X509TrustManager() {
      @Override
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return defaultTrustManager.getAcceptedIssuers();
      }

      @Override
      public void checkClientTrusted(
          java.security.cert.X509Certificate[] certs, String authType) {
        // 客户端信任检查当前未使用
      }

      @Override
      public void checkServerTrusted(
          java.security.cert.X509Certificate[] certs, String authType)