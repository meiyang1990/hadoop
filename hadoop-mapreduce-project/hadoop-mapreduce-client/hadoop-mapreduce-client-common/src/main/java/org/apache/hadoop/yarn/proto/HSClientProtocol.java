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

/**
 * 历史服务器客户端协议protobuf实现模块，负责区分安全类加载器中的阻塞接口
 */
package org.apache.hadoop.yarn.proto;

import org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.yarn.proto.MRClientProtocol.MRClientProtocolService;

/**
 * 伪协议接口，用于在安全信息类加载器中区分不同的阻塞接口，避免类加载冲突
 * 为MapReduce历史服务器客户端协议提供Protobuf服务工厂能力
 */
public interface HSClientProtocol {
  /**
   * 历史服务器客户端协议Protobuf服务工厂类，提供阻塞服务实例创建能力
   */
  public abstract class HSClientProtocolService {
    /**
     * 历史服务器客户端协议阻塞接口，继承MapReduce客户端协议PB接口
     */
    public interface BlockingInterface extends MRClientProtocolPB {
    }

    /**
     * 通过反射方式创建Protobuf阻塞服务实例
     * @param impl 阻塞接口实现实例
     * @return Protobuf阻塞服务实例
     */
    public static BlockingService newReflectiveBlockingService(
        final HSClientProtocolService.BlockingInterface impl) {
      // 类型转换安全，因为实现类已经继承了正确的接口
      return MRClientProtocolService
          .newReflectiveBlockingService((MRClientProtocolService.BlockingInterface) impl);
    }
  }
}