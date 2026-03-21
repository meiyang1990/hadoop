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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.io.Serializable;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

/**
 * YARN NodeManager 设备插件框架中，对单个硬件设备资源的抽象表示。
 * 用于描述节点上可分配给容器的GPU、FPGA等外设资源信息。
 * */
@XmlAccessorType(XmlAccessType.FIELD)
public final class Device implements Serializable, Comparable {

  private static final long serialVersionUID = -7270474563684671656L;

  /**
   * 设备插件指定的设备索引，必须设置，推荐从0开始编号。
   * */
  private int id;

  /**
   * 设备在宿主机上的设备文件路径，例如"/dev/nvidia0"，可选字段。
   * */
  private String devPath;

  /**
   * Linux设备主设备号，可选字段。
   * */
  private int majorNumber;

  /**
   * Linux设备次设备号，可选字段。
   * */
  private int minorNumber;

  /**
   * PCI总线地址，格式为[[[<domain>]:]<bus>]:][<slot>][.[<func>]]，
   * 可通过Linux命令lspci -D获取，可选字段。
   * */
  private String busID;

  /**
   * 设备健康状态标记，false表示不健康，默认值为false。
   * */
  private boolean isHealthy;

  /**
   * 设备插件自定义的状态描述信息，可选字段。
   * */
  private String status;

  /**
   * 私有构造函数，通过Builder构造Device实例，必填校验设备ID。
   * @param builder 设备构造器对象
   */
  private Device(Builder builder) {
    if (builder.id == -1) {
      throw new IllegalArgumentException("Please set the id for Device");
    }
    this.id = builder.id;
    this.devPath = builder.devPath;
    this.majorNumber = builder.majorNumber;
    this.minorNumber = builder.minorNumber;
    this.busID = builder.busID;
    this.isHealthy = builder.isHealthy;
    this.status = builder.status;
  }

  private Device() {
  }

  public int getId() {
    return id;
  }

  public String getDevPath() {
    return devPath;
  }

  public int getMajorNumber() {
    return majorNumber;
  }

  public int getMinorNumber() {
    return minorNumber;
  }

  public String getBusID() {
    return busID;
  }

  public boolean isHealthy() {
    return isHealthy;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Device device = (Device) o;
    return id == device.getId()
        && Objects.equals(devPath, device.getDevPath())
        && majorNumber == device.getMajorNumber()
        && minorNumber == device.getMinorNumber()
        && Objects.equals(busID, device.getBusID());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, devPath, majorNumber, minorNumber, busID);
  }

  @Override
  public int compareTo(Object o) {
    if (o == null || (!(o instanceof Device))) {
      return -1;
    }

    Device other = (Device) o;

    // 优先按设备ID比较
    int result = Integer.compare(id, other.getId());
    if (0 != result) {
      return result;
    }

    // 其次按主设备号比较
    result = Integer.compare(majorNumber, other.getMajorNumber());
    if (0 != result) {
      return result;
    }

    // 再按次设备号比较
    result = Integer.compare(minorNumber, other.getMinorNumber());
    if (0 != result) {
      return result;
    }

    // 再按设备路径比较
    result = devPath.compareTo(other.getDevPath());
    if (0 != result) {
      return result;
    }

    // 最后按PCI总线ID比较
    return busID.compareTo(other.getBusID());
  }

  @Override
  public String toString() {
    return "(" + getId() + ", " + getDevPath() + ", "
        + getMajorNumber() + ":" + getMinorNumber() + ")";
  }

  /**
   * Device对象的Builder构造器，用于链式构造Device实例。
   * */
  public final static class Builder {
    // 默认-1表示该字段未设置
    private int id = -1;
    private String devPath = "";
    private int majorNumber = -1;
    private int minorNumber = -1;
    private String busID = "";
    private boolean isHealthy;
    private String status = "";

    public static Builder newInstance() {
      return new Builder();
    }

    public Device build() {
      return new Device(this);
    }

    public Builder setId(int i) {
      this.id = i;
      return this;
    }

    public Builder setDevPath(String dp) {
      this.devPath = dp;
      return this;
    }

    public Builder setMajorNumber(int maN) {
      this.majorNumber = maN;
      return this;
    }

    public Builder setMinorNumber(int miN) {
      this.minorNumber = miN;
      return this;
    }

    public Builder setBusID(String bI) {
      this.busID = bI;
      return this;
    }

    public Builder setHealthy(boolean healthy) {
      isHealthy = healthy;
      return this;
    }

    public Builder setStatus(String s) {
      this.status = s;
      return this;
    }

  }
}