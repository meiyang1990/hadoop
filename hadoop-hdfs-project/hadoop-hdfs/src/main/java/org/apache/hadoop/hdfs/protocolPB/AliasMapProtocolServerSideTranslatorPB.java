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
package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.KeyValueProto;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.ReadResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.WriteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.WriteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.common.FileRegion;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.*;
import static org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap.*;

/**
 * 文件级注释：别名映射协议Protobuf服务端翻译器，负责将Protobuf格式的RPC请求转换为内部对象模型，
 * 转发给内存别名映射服务处理，再将处理结果转换回Protobuf格式返回给客户端，用于外部提供存储块地址映射服务
 */
/**
 * AliasMapProtocolServerSideTranslatorPB is responsible for translating RPC
 * calls and forwarding them to the internal InMemoryAliasMap.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
/**
 * 别名映射协议Protobuf服务端翻译类，实现Protobuf RPC协议转换，连接外部RPC层和内部别名映射服务
 */
public class AliasMapProtocolServerSideTranslatorPB
    implements AliasMapProtocolPB {

  private final InMemoryAliasMapProtocol aliasMap;

  /**
   * 构造函数，基于内部别名映射服务实例创建协议转换器
   * @param aliasMap 内部内存别名映射服务实例
   */
  public AliasMapProtocolServerSideTranslatorPB(
      InMemoryAliasMapProtocol aliasMap) {
    this.aliasMap = aliasMap;
  }

  // 空写响应单例，写操作无返回数据时复用此对象
  private static final WriteResponseProto VOID_WRITE_RESPONSE =
      WriteResponseProto.newBuilder().build();

  /**
   * 处理写别名映射RPC请求，完成协议转换并转发给内部服务
   * @param controller RPC控制器
   * @param request Protobuf格式写请求
   * @return Protobuf格式写响应
   * @throws ServiceException 服务异常，封装内部IO异常
   */
  @Override
  public WriteResponseProto write(RpcController controller,
      WriteRequestProto request) throws ServiceException {
    try {
      // 将Protobuf请求转换为内部FileRegion对象
      FileRegion toWrite =
          PBHelper.convert(request.getKeyValuePair());

      // 调用内部服务写入块与提供存储位置的映射关系
      aliasMap.write(toWrite.getBlock(), toWrite.getProvidedStorageLocation());
      return VOID_WRITE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 处理读别名映射RPC请求，完成协议转换并转发给内部服务
   * @param controller RPC控制器
   * @param request Protobuf格式读请求
   * @return Protobuf格式读响应，包含查询到的存储位置（如果存在）
   * @throws ServiceException 服务异常，封装内部IO异常
   */
  @Override
  public ReadResponseProto read(RpcController controller,
      ReadRequestProto request) throws ServiceException {
    try {
      // 将Protobuf格式块键转换为内部Block对象
      Block toRead =  PBHelperClient.convert(request.getKey());

      // 调用内部服务查询块对应的存储位置
      Optional<ProvidedStorageLocation> optionalResult =
          aliasMap.read(toRead);

      ReadResponseProto.Builder builder = ReadResponseProto.newBuilder();
      // 如果查询到结果，转换为Protobuf格式放入响应
      if (optionalResult.isPresent()) {
        ProvidedStorageLocation providedStorageLocation = optionalResult.get();
        builder.setValue(PBHelperClient.convert(providedStorageLocation));
      }

      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 处理分页列出别名映射条目RPC请求，完成协议转换并转发给内部服务
   * @param controller RPC控制器
   * @param request Protobuf格式列表请求，包含分页标记
   * @return Protobuf格式列表响应，包含当前页条目和下一页标记
   * @throws ServiceException 服务异常，封装内部IO异常
   */
  @Override
  public ListResponseProto list(RpcController controller,
      ListRequestProto request) throws ServiceException {
    try {
      BlockProto marker = request.getMarker();
      IterationResult iterationResult;
      // 根据标记是否初始化，判断是否为第一页查询
      if (marker.isInitialized()) {
        // 非第一页，转换标记后调用带标记的列表方法
        iterationResult =
            aliasMap.list(Optional.of(PBHelperClient.convert(marker)));
      } else {
        // 第一页，无起始标记
        iterationResult = aliasMap.list(Optional.empty());
      }
      ListResponseProto.Builder responseBuilder =
          ListResponseProto.newBuilder();
      List<FileRegion> fileRegions = iterationResult.getFileRegions();

      // 将所有条目转换为Protobuf格式添加到响应
      List<KeyValueProto> keyValueProtos = fileRegions.stream()
          .map(PBHelper::convert).collect(Collectors.toList());
      responseBuilder.addAllFileRegions(keyValueProtos);
      // 如果存在下一页标记，转换后添加到响应
      Optional<Block> nextMarker = iterationResult.getNextBlock();
      nextMarker
          .map(m -> responseBuilder.setNextMarker(PBHelperClient.convert(m)));

      return responseBuilder.build();

    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 处理获取块池IDRPC请求，完成协议转换并转发给内部服务
   * @param controller RPC控制器
   * @param req Protobuf格式块池请求
   * @return Protobuf格式块池响应，包含当前别名映射服务所属的块池ID
   * @throws ServiceException 服务异常，封装内部IO异常
   */
  public BlockPoolResponseProto getBlockPoolId(RpcController controller,
      BlockPoolRequestProto req) throws ServiceException {
    try {
      // 调用内部服务获取块池ID
      String bpid = aliasMap.getBlockPoolId();
      // 构建Protobuf响应返回
      return BlockPoolResponseProto.newBuilder().setBlockPoolId(bpid).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}