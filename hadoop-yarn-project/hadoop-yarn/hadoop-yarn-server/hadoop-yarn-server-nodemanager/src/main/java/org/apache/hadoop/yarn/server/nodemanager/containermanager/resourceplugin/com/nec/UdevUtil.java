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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nec;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

/**
 * 通过JNA调用Linux libudev库，获取设备系统路径的工具类
 * 用于NEC外部设备资源管理，查询设备在sys文件系统中的路径
 */
class UdevUtil {
  private static LibUdev libUdev;

  /**
   * 初始化udev库，线程安全
   */
  public synchronized void init() {
    LibUdev.init();
    libUdev = LibUdev.instance;
  }

  /**
   * 根据设备号和设备类型，查询设备在sys文件系统中的路径
   * @param deviceNo 设备号
   * @param devType 设备类型（'c'表示字符设备，'b'表示块设备）
   * @return 设备sysfs路径
   * @throws IllegalArgumentException 设备或路径不存在时抛出异常
   */
  public String getSysPath(int deviceNo, char devType) {
    Pointer udev = null;
    Pointer device = null;

    try {
      // 创建新的udev上下文
      udev = libUdev.udev_new();
      // 根据设备号创建设备对象
      device = libUdev.udev_device_new_from_devnum(
          udev, (byte)devType, deviceNo);
      if (device == null) {
        throw new IllegalArgumentException("Udev: device not found");
      }
      // 获取设备的sysfs路径指针
      Pointer sysPathPtr = libUdev.udev_device_get_syspath(device);
      if (sysPathPtr == null) {
        throw new IllegalArgumentException(
            "Udev: syspath not found for device");
      }
      // 转换为Java字符串返回
      return sysPathPtr.getString(0);
    } finally {
      // 释放设备对象引用
      if (device != null) {
        libUdev.udev_device_unref(device);
      }
      // 释放udev上下文引用
      if (udev != null) {
        libUdev.udev_unref(udev);
      }
    }
  }

  @SuppressWarnings({"checkstyle:staticvariablename", "checkstyle:methodname",
      "checkstyle:parametername"})
  /**
   * libudev库的JNA实现类，实现对原生udev接口的映射
   */
  private static class LibUdev implements LibUdevMapping {
    private static LibUdev instance;

    /**
     * 单例初始化，注册udev原生库
     */
    public static void init() {
      if (instance == null) {
        Native.register("udev");
        instance = new LibUdev();
      }
    }

    public native Pointer udev_new();

    public native Pointer udev_unref(Pointer udev);

    public native Pointer udev_device_new_from_devnum(Pointer udev,
        byte type,
        int devnum);

    public native Pointer udev_device_get_syspath(Pointer udev_device);

    public native Pointer udev_device_unref(Pointer udev_device);
  }

  @SuppressWarnings({"checkstyle:staticvariablename", "checkstyle:methodname",
      "checkstyle:parametername"})
  /**
   * libudev原生接口方法定义接口，定义所有需要调用的udev函数签名
   */
  interface LibUdevMapping {
    Pointer udev_new();

    Pointer udev_unref(Pointer udev);

    Pointer udev_device_new_from_devnum(Pointer udev,
        byte type,
        int devnum);

    Pointer udev_device_get_syspath(Pointer udev_device);

    Pointer udev_device_unref(Pointer udev_device);
  }
}