#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hadoop HDFS 核心架构设计文档生成脚本
基于 release-3.3.5-RC0 版本源码分析
"""

import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, grey
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Line, Rect, String, Group
from reportlab.graphics import renderPDF

# ============================================================
# 字体注册
# ============================================================
def register_fonts():
    """注册中文字体"""
    font_paths = [
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Supplemental/Songti.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
    ]
    registered = False
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont('ChineseFont', fp))
                pdfmetrics.registerFont(TTFont('ChineseFontBold', fp))
                registered = True
                break
            except Exception:
                continue

    if not registered:
        try:
            pdfmetrics.registerFont(TTFont('ChineseFont', 'STHeiti Light.ttc'))
            pdfmetrics.registerFont(TTFont('ChineseFontBold', 'STHeiti Light.ttc'))
        except Exception:
            print("WARNING: Chinese fonts not found, using Helvetica")
            pdfmetrics.registerAlias('ChineseFont', 'Helvetica')
            pdfmetrics.registerAlias('ChineseFontBold', 'Helvetica-Bold')

register_fonts()

# ============================================================
# 样式定义
# ============================================================
styles = getSampleStyleSheet()

DARK_BLUE = HexColor('#1a237e')
MEDIUM_BLUE = HexColor('#1565c0')
LIGHT_BLUE = HexColor('#e3f2fd')
ACCENT_GREEN = HexColor('#2e7d32')
ACCENT_ORANGE = HexColor('#e65100')
TABLE_HEADER_BG = HexColor('#1565c0')
TABLE_ALT_ROW = HexColor('#f5f5f5')
BORDER_COLOR = HexColor('#bdbdbd')

style_title = ParagraphStyle('DocTitle', fontName='ChineseFontBold', fontSize=28,
    leading=36, alignment=TA_CENTER, textColor=DARK_BLUE, spaceAfter=6*mm)
style_subtitle = ParagraphStyle('DocSubtitle', fontName='ChineseFont', fontSize=14,
    leading=20, alignment=TA_CENTER, textColor=HexColor('#616161'), spaceAfter=10*mm)
style_h1 = ParagraphStyle('H1', fontName='ChineseFontBold', fontSize=20,
    leading=28, textColor=DARK_BLUE, spaceBefore=12*mm, spaceAfter=6*mm,
    borderWidth=0, borderPadding=0, leftIndent=0)
style_h2 = ParagraphStyle('H2', fontName='ChineseFontBold', fontSize=16,
    leading=22, textColor=MEDIUM_BLUE, spaceBefore=8*mm, spaceAfter=4*mm)
style_h3 = ParagraphStyle('H3', fontName='ChineseFontBold', fontSize=13,
    leading=18, textColor=ACCENT_GREEN, spaceBefore=6*mm, spaceAfter=3*mm)
style_body = ParagraphStyle('Body', fontName='ChineseFont', fontSize=10.5,
    leading=17, alignment=TA_JUSTIFY, spaceBefore=1*mm, spaceAfter=2*mm)
style_code = ParagraphStyle('Code', fontName='Courier', fontSize=9,
    leading=13, leftIndent=10*mm, backColor=HexColor('#f5f5f5'),
    borderWidth=0.5, borderColor=BORDER_COLOR, borderPadding=4)
style_bullet = ParagraphStyle('Bullet', fontName='ChineseFont', fontSize=10.5,
    leading=16, leftIndent=12*mm, bulletIndent=6*mm, spaceBefore=1*mm, spaceAfter=1*mm)
style_toc = ParagraphStyle('TOC', fontName='ChineseFont', fontSize=12,
    leading=20, leftIndent=8*mm, spaceBefore=2*mm, spaceAfter=2*mm, textColor=MEDIUM_BLUE)
style_table_header = ParagraphStyle('TH', fontName='ChineseFontBold', fontSize=10,
    leading=14, alignment=TA_CENTER, textColor=white)
style_table_cell = ParagraphStyle('TC', fontName='ChineseFont', fontSize=9.5,
    leading=14, alignment=TA_LEFT)
style_caption = ParagraphStyle('Caption', fontName='ChineseFont', fontSize=9,
    leading=13, alignment=TA_CENTER, textColor=HexColor('#757575'),
    spaceBefore=2*mm, spaceAfter=4*mm)

# ============================================================
# 辅助函数
# ============================================================
def make_table(headers, rows, col_widths=None):
    """创建带样式的表格"""
    hdr = [Paragraph(h, style_table_header) for h in headers]
    body = []
    for row in rows:
        body.append([Paragraph(str(c), style_table_cell) for c in row])
    data = [hdr] + body
    if col_widths is None:
        col_widths = [170*mm / len(headers)] * len(headers)
    t = Table(data, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ('BACKGROUND', (0, 0), (-1, 0), TABLE_HEADER_BG),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, -1), 'ChineseFont'),
        ('FONTSIZE', (0, 0), (-1, -1), 9.5),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, BORDER_COLOR),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 5),
        ('TOPPADDING', (0, 1), (-1, -1), 5),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(('BACKGROUND', (0, i), (-1, i), TABLE_ALT_ROW))
    t.setStyle(TableStyle(style_cmds))
    return t

def draw_arch_diagram():
    """绘制 HDFS 整体架构图"""
    d = Drawing(480, 380)
    # 背景
    d.add(Rect(0, 0, 480, 380, fillColor=HexColor('#fafafa'), strokeColor=None))

    # 标题
    d.add(String(240, 365, 'HDFS 核心架构', fontName='ChineseFontBold', fontSize=14,
                 fillColor=DARK_BLUE, textAnchor='middle'))

    # Client 层
    d.add(Rect(30, 310, 120, 35, fillColor=HexColor('#e8f5e9'), strokeColor=ACCENT_GREEN, rx=5))
    d.add(String(90, 323, 'HDFS Client', fontName='ChineseFontBold', fontSize=10,
                 fillColor=ACCENT_GREEN, textAnchor='middle'))

    d.add(Rect(180, 310, 120, 35, fillColor=HexColor('#e8f5e9'), strokeColor=ACCENT_GREEN, rx=5))
    d.add(String(240, 323, 'DFSClient', fontName='ChineseFontBold', fontSize=10,
                 fillColor=ACCENT_GREEN, textAnchor='middle'))

    d.add(Rect(330, 310, 120, 35, fillColor=HexColor('#e8f5e9'), strokeColor=ACCENT_GREEN, rx=5))
    d.add(String(390, 323, 'WebHDFS', fontName='ChineseFontBold', fontSize=10,
                 fillColor=ACCENT_GREEN, textAnchor='middle'))

    # NameNode 层（大框）
    d.add(Rect(20, 155, 440, 140, fillColor=HexColor('#e3f2fd'), strokeColor=MEDIUM_BLUE,
               strokeWidth=2, rx=8))
    d.add(String(240, 280, 'NameNode', fontName='ChineseFontBold', fontSize=13,
                 fillColor=DARK_BLUE, textAnchor='middle'))

    # NameNode 内部组件
    nn_components = [
        (35, 235, 95, 30, 'FSNamesystem'),
        (140, 235, 95, 30, 'FSDirectory'),
        (245, 235, 95, 30, 'BlockManager'),
        (350, 235, 95, 30, 'LeaseManager'),
        (35, 195, 95, 30, 'FSEditLog'),
        (140, 195, 95, 30, 'FSImage'),
        (245, 195, 95, 30, 'DatanodeManager'),
        (350, 195, 95, 30, 'NameNodeRpcServer'),
        (140, 160, 100, 28, 'HA (Active/Standby)'),
        (260, 160, 100, 28, 'SnapshotManager'),
    ]
    for x, y, w, h, label in nn_components:
        d.add(Rect(x, y, w, h, fillColor=white, strokeColor=MEDIUM_BLUE, rx=3))
        d.add(String(x + w/2, y + h/2 - 4, label, fontName='ChineseFont', fontSize=8,
                     fillColor=DARK_BLUE, textAnchor='middle'))

    # DataNode 层
    dn_y = 30
    dn_h = 110
    d.add(Rect(20, dn_y, 440, dn_h, fillColor=HexColor('#fff3e0'), strokeColor=ACCENT_ORANGE,
               strokeWidth=2, rx=8))
    d.add(String(240, dn_y + dn_h - 15, 'DataNode 集群', fontName='ChineseFontBold', fontSize=13,
                 fillColor=ACCENT_ORANGE, textAnchor='middle'))

    # DataNode 内部组件
    dn_components = [
        (35, dn_y + 50, 90, 28, 'BPServiceActor'),
        (135, dn_y + 50, 90, 28, 'DataXceiver'),
        (235, dn_y + 50, 90, 28, 'BlockReceiver'),
        (335, dn_y + 50, 110, 28, 'BlockPoolManager'),
        (35, dn_y + 10, 90, 28, 'FsDatasetImpl'),
        (135, dn_y + 10, 90, 28, 'FsVolumeList'),
        (235, dn_y + 10, 90, 28, 'EC Worker'),
        (335, dn_y + 10, 110, 28, 'DataXceiverServer'),
    ]
    for x, y, w, h, label in dn_components:
        d.add(Rect(x, y, w, h, fillColor=white, strokeColor=ACCENT_ORANGE, rx=3))
        d.add(String(x + w/2, y + h/2 - 4, label, fontName='ChineseFont', fontSize=8,
                     fillColor=ACCENT_ORANGE, textAnchor='middle'))

    # 连接线 (Client -> NameNode)
    d.add(Line(90, 310, 90, 295, strokeColor=grey, strokeWidth=1.5))
    d.add(Line(240, 310, 240, 295, strokeColor=grey, strokeWidth=1.5))

    # 连接线 (NameNode -> DataNode)
    d.add(Line(150, 155, 150, 140, strokeColor=grey, strokeWidth=1.5, strokeDashArray=[4, 2]))
    d.add(Line(300, 155, 300, 140, strokeColor=grey, strokeWidth=1.5, strokeDashArray=[4, 2]))

    return d

def draw_ha_state_diagram():
    """绘制 HA 状态机转换图"""
    d = Drawing(440, 220)
    d.add(Rect(0, 0, 440, 220, fillColor=HexColor('#fafafa'), strokeColor=None))
    d.add(String(220, 205, 'HA 状态机转换图（状态模式）', fontName='ChineseFontBold', fontSize=12,
                 fillColor=DARK_BLUE, textAnchor='middle'))

    # Standby 状态
    d.add(Rect(30, 100, 110, 60, fillColor=HexColor('#e3f2fd'), strokeColor=MEDIUM_BLUE,
               strokeWidth=2, rx=8))
    d.add(String(85, 138, 'StandbyState', fontName='ChineseFontBold', fontSize=10,
                 fillColor=MEDIUM_BLUE, textAnchor='middle'))
    d.add(String(85, 118, 'startStandbyServices', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))
    d.add(String(85, 108, '拒绝WRITE操作', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))

    # Active 状态
    d.add(Rect(300, 100, 110, 60, fillColor=HexColor('#e8f5e9'), strokeColor=ACCENT_GREEN,
               strokeWidth=2, rx=8))
    d.add(String(355, 138, 'ActiveState', fontName='ChineseFontBold', fontSize=10,
                 fillColor=ACCENT_GREEN, textAnchor='middle'))
    d.add(String(355, 118, 'startActiveServices', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))
    d.add(String(355, 108, '允许所有操作', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))

    # Observer 状态
    d.add(Rect(160, 20, 120, 50, fillColor=HexColor('#fff3e0'), strokeColor=ACCENT_ORANGE,
               strokeWidth=2, rx=8))
    d.add(String(220, 50, 'ObserverState', fontName='ChineseFontBold', fontSize=10,
                 fillColor=ACCENT_ORANGE, textAnchor='middle'))
    d.add(String(220, 35, 'WRITE重定向到Active', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))

    # 转换箭头
    # Standby -> Active
    d.add(Line(140, 140, 300, 140, strokeColor=ACCENT_GREEN, strokeWidth=2))
    d.add(String(220, 148, 'failover', fontName='ChineseFont', fontSize=8, fillColor=ACCENT_GREEN,
                 textAnchor='middle'))
    # Active -> Standby
    d.add(Line(300, 115, 140, 115, strokeColor=MEDIUM_BLUE, strokeWidth=2))
    d.add(String(220, 103, 'graceful failover', fontName='ChineseFont', fontSize=8,
                 fillColor=MEDIUM_BLUE, textAnchor='middle'))
    # Standby -> Observer
    d.add(Line(85, 100, 170, 70, strokeColor=ACCENT_ORANGE, strokeWidth=1.5,
               strokeDashArray=[4, 2]))
    # Observer -> Standby
    d.add(Line(170, 60, 95, 100, strokeColor=MEDIUM_BLUE, strokeWidth=1.5,
               strokeDashArray=[4, 2]))

    # HAState 基类
    d.add(Rect(150, 170, 140, 30, fillColor=HexColor('#f3e5f5'), strokeColor=HexColor('#7b1fa2'),
               rx=5))
    d.add(String(220, 180, 'HAState (抽象基类)', fontName='ChineseFontBold', fontSize=9,
                 fillColor=HexColor('#7b1fa2'), textAnchor='middle'))
    d.add(Line(220, 170, 85, 160, strokeColor=grey, strokeWidth=1, strokeDashArray=[3, 2]))
    d.add(Line(220, 170, 355, 160, strokeColor=grey, strokeWidth=1, strokeDashArray=[3, 2]))
    d.add(Line(220, 170, 220, 70, strokeColor=grey, strokeWidth=1, strokeDashArray=[3, 2]))

    return d

def draw_write_pipeline():
    """绘制数据写入 Pipeline 时序图"""
    d = Drawing(480, 300)
    d.add(Rect(0, 0, 480, 300, fillColor=HexColor('#fafafa'), strokeColor=None))
    d.add(String(240, 285, '数据写入 Pipeline 流程', fontName='ChineseFontBold', fontSize=12,
                 fillColor=DARK_BLUE, textAnchor='middle'))

    # 参与者
    actors = [
        (60, 'Client'),
        (160, 'NameNode'),
        (270, 'DataNode1'),
        (370, 'DataNode2'),
        (450, 'DataNode3'),
    ]
    for x, name in actors:
        d.add(Rect(x-35, 250, 70, 25, fillColor=LIGHT_BLUE, strokeColor=MEDIUM_BLUE, rx=4))
        d.add(String(x, 258, name, fontName='ChineseFontBold', fontSize=9,
                     fillColor=DARK_BLUE, textAnchor='middle'))
        d.add(Line(x, 250, x, 20, strokeColor=grey, strokeWidth=0.8, strokeDashArray=[3, 2]))

    # 消息
    msgs = [
        (60, 160, 235, 'create(path)', ACCENT_GREEN),
        (160, 60, 225, 'addBlock() → 分配块+选择DN', MEDIUM_BLUE),
        (60, 270, 215, 'writeBlock (pipeline)', ACCENT_ORANGE),
        (270, 370, 200, 'forward', ACCENT_ORANGE),
        (370, 450, 190, 'forward', ACCENT_ORANGE),
        (450, 370, 175, 'ACK', ACCENT_GREEN),
        (370, 270, 165, 'ACK', ACCENT_GREEN),
        (270, 60, 155, 'ACK', ACCENT_GREEN),
        (60, 160, 140, 'complete()', MEDIUM_BLUE),
    ]
    for x1, x2, y, label, color in msgs:
        d.add(Line(x1, y, x2, y, strokeColor=color, strokeWidth=1.5))
        lx = (x1 + x2) / 2
        d.add(String(lx, y + 4, label, fontName='ChineseFont', fontSize=7,
                     fillColor=color, textAnchor='middle'))

    return d

def draw_inode_hierarchy():
    """绘制 INode 类层次结构图"""
    d = Drawing(440, 220)
    d.add(Rect(0, 0, 440, 220, fillColor=HexColor('#fafafa'), strokeColor=None))
    d.add(String(220, 205, 'INode 类继承层次', fontName='ChineseFontBold', fontSize=12,
                 fillColor=DARK_BLUE, textAnchor='middle'))

    # INode
    d.add(Rect(170, 170, 100, 25, fillColor=HexColor('#f3e5f5'), strokeColor=HexColor('#7b1fa2'), rx=5))
    d.add(String(220, 178, 'INode (抽象)', fontName='ChineseFontBold', fontSize=9,
                 fillColor=HexColor('#7b1fa2'), textAnchor='middle'))

    # INodeWithAdditionalFields
    d.add(Rect(120, 125, 200, 30, fillColor=HexColor('#e8eaf6'), strokeColor=MEDIUM_BLUE, rx=5))
    d.add(String(220, 138, 'INodeWithAdditionalFields', fontName='ChineseFontBold', fontSize=9,
                 fillColor=DARK_BLUE, textAnchor='middle'))
    d.add(String(220, 128, 'id, name, permission(64bit), time', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))
    d.add(Line(220, 170, 220, 155, strokeColor=grey, strokeWidth=1))

    # INodeFile
    d.add(Rect(30, 60, 170, 45, fillColor=HexColor('#e8f5e9'), strokeColor=ACCENT_GREEN, rx=5))
    d.add(String(115, 90, 'INodeFile', fontName='ChineseFontBold', fontSize=10,
                 fillColor=ACCENT_GREEN, textAnchor='middle'))
    d.add(String(115, 78, 'header(64bit), blocks[]', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))
    d.add(String(115, 68, 'Replica/EC, BlockSize', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))
    d.add(Line(160, 125, 115, 105, strokeColor=grey, strokeWidth=1))

    # INodeDirectory
    d.add(Rect(240, 60, 180, 45, fillColor=HexColor('#fff3e0'), strokeColor=ACCENT_ORANGE, rx=5))
    d.add(String(330, 90, 'INodeDirectory', fontName='ChineseFontBold', fontSize=10,
                 fillColor=ACCENT_ORANGE, textAnchor='middle'))
    d.add(String(330, 78, 'children: List<INode>', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))
    d.add(String(330, 68, 'SnapshottableFeature', fontName='ChineseFont', fontSize=7,
                 fillColor=grey, textAnchor='middle'))
    d.add(Line(280, 125, 330, 105, strokeColor=grey, strokeWidth=1))

    # INodeReference
    d.add(Rect(140, 5, 160, 30, fillColor=HexColor('#fce4ec'), strokeColor=HexColor('#c62828'), rx=5))
    d.add(String(220, 18, 'INodeReference (快照引用)', fontName='ChineseFontBold', fontSize=9,
                 fillColor=HexColor('#c62828'), textAnchor='middle'))
    d.add(Line(220, 125, 220, 35, strokeColor=grey, strokeWidth=1, strokeDashArray=[3, 2]))

    return d

def draw_editlog_buffer():
    """绘制 EditLog 双缓冲区同步机制图"""
    d = Drawing(440, 180)
    d.add(Rect(0, 0, 440, 180, fillColor=HexColor('#fafafa'), strokeColor=None))
    d.add(String(220, 165, 'FSEditLog 双缓冲区 + 批量同步机制', fontName='ChineseFontBold', fontSize=12,
                 fillColor=DARK_BLUE, textAnchor='middle'))

    # Buffer A (写入缓冲区)
    d.add(Rect(30, 90, 160, 55, fillColor=HexColor('#e8f5e9'), strokeColor=ACCENT_GREEN,
               strokeWidth=2, rx=5))
    d.add(String(110, 130, '写入缓冲区 (Buffer A)', fontName='ChineseFontBold', fontSize=9,
                 fillColor=ACCENT_GREEN, textAnchor='middle'))
    d.add(String(110, 115, 'logEdit() → write(op)', fontName='ChineseFont', fontSize=8,
                 fillColor=grey, textAnchor='middle'))
    d.add(String(110, 100, '多线程并发写入', fontName='ChineseFont', fontSize=8,
                 fillColor=grey, textAnchor='middle'))

    # Buffer B (刷盘缓冲区)
    d.add(Rect(250, 90, 160, 55, fillColor=HexColor('#fff3e0'), strokeColor=ACCENT_ORANGE,
               strokeWidth=2, rx=5))
    d.add(String(330, 130, '刷盘缓冲区 (Buffer B)', fontName='ChineseFontBold', fontSize=9,
                 fillColor=ACCENT_ORANGE, textAnchor='middle'))
    d.add(String(330, 115, 'logSync() → flush()', fontName='ChineseFont', fontSize=8,
                 fillColor=grey, textAnchor='middle'))
    d.add(String(330, 100, '持久化到 Journal', fontName='ChineseFont', fontSize=8,
                 fillColor=grey, textAnchor='middle'))

    # swap 箭头
    d.add(Line(190, 125, 250, 125, strokeColor=MEDIUM_BLUE, strokeWidth=2))
    d.add(String(220, 132, 'swap', fontName='ChineseFontBold', fontSize=8,
                 fillColor=MEDIUM_BLUE, textAnchor='middle'))
    d.add(Line(250, 105, 190, 105, strokeColor=MEDIUM_BLUE, strokeWidth=2))

    # 目标存储
    targets = [
        (80, 30, 'EditLog File'),
        (220, 30, 'JournalNode 1'),
        (360, 30, 'JournalNode 2'),
    ]
    for x, y, label in targets:
        d.add(Rect(x-50, y, 100, 25, fillColor=HexColor('#e3f2fd'), strokeColor=MEDIUM_BLUE, rx=4))
        d.add(String(x, y+8, label, fontName='ChineseFont', fontSize=8,
                     fillColor=MEDIUM_BLUE, textAnchor='middle'))

    d.add(Line(330, 90, 80, 55, strokeColor=grey, strokeWidth=1, strokeDashArray=[3, 2]))
    d.add(Line(330, 90, 220, 55, strokeColor=grey, strokeWidth=1, strokeDashArray=[3, 2]))
    d.add(Line(330, 90, 360, 55, strokeColor=grey, strokeWidth=1, strokeDashArray=[3, 2]))

    return d

# ============================================================
# 页眉页脚
# ============================================================
def on_first_page(canvas, doc):
    canvas.saveState()
    canvas.setFont('ChineseFont', 8)
    canvas.setFillColor(HexColor('#9e9e9e'))
    canvas.drawCentredString(A4[0]/2, 15*mm, 'Hadoop HDFS 核心架构设计文档 | release-3.3.5-RC0')
    canvas.restoreState()

def on_later_pages(canvas, doc):
    canvas.saveState()
    canvas.setFont('ChineseFont', 8)
    canvas.setFillColor(HexColor('#9e9e9e'))
    canvas.drawString(20*mm, A4[1] - 12*mm, 'Hadoop HDFS 核心架构设计文档')
    canvas.drawRightString(A4[0] - 20*mm, A4[1] - 12*mm, f'第 {doc.page} 页')
    canvas.line(20*mm, A4[1] - 14*mm, A4[0] - 20*mm, A4[1] - 14*mm)
    canvas.drawCentredString(A4[0]/2, 15*mm, 'Hadoop HDFS 核心架构设计文档 | release-3.3.5-RC0')
    canvas.restoreState()

# ============================================================
# 构建文档内容
# ============================================================
def build_content():
    elements = []

    # ======================== 封面 ========================
    elements.append(Spacer(1, 50*mm))
    elements.append(Paragraph('Hadoop HDFS', style_title))
    elements.append(Paragraph('核心架构设计文档', style_title))
    elements.append(Spacer(1, 8*mm))
    elements.append(Paragraph('基于 release-3.3.5-RC0 版本源码深度分析', style_subtitle))
    elements.append(Spacer(1, 15*mm))

    info_data = [
        ['项目', 'Apache Hadoop HDFS'],
        ['模块', 'hadoop-hdfs-project / hadoop-hdfs'],
        ['分支版本', 'release-3.3.5-RC0'],
        ['源文件总数', '~716 个 Java 文件'],
        ['分析范围', 'NameNode / DataNode / BlockManager / HA / Snapshot / EC'],
        ['文档生成', '自动化源码分析工具'],
    ]
    info_table = Table(
        [[Paragraph(r[0], style_table_header), Paragraph(r[1], style_table_cell)] for r in info_data],
        colWidths=[55*mm, 115*mm]
    )
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), TABLE_HEADER_BG),
        ('TEXTCOLOR', (0, 0), (0, -1), white),
        ('BACKGROUND', (1, 0), (1, -1), white),
        ('GRID', (0, 0), (-1, -1), 0.5, BORDER_COLOR),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    elements.append(info_table)
    elements.append(PageBreak())

    # ======================== 目录 ========================
    elements.append(Paragraph('目录', style_h1))
    toc_items = [
        '一、HDFS 整体架构概览',
        '二、模块源码结构分析',
        '三、NameNode 核心架构',
        '四、DataNode 核心架构',
        '五、BlockManager 块管理',
        '六、HA 高可用机制（状态模式）',
        '七、文件系统树（INode 层次结构）',
        '八、编辑日志与持久化机制',
        '九、核心流程时序分析',
        '十、设计模式总结',
        '十一、关键场景调用链',
        '十二、核心源文件清单',
    ]
    for item in toc_items:
        elements.append(Paragraph(item, style_toc))
    elements.append(PageBreak())

    # ======================== 一、整体架构概览 ========================
    elements.append(Paragraph('一、HDFS 整体架构概览', style_h1))
    elements.append(Paragraph(
        'HDFS（Hadoop Distributed File System）是 Apache Hadoop 的核心存储组件，'
        '采用主从架构（Master/Slave），由一个 NameNode 和多个 DataNode 组成。'
        'NameNode 负责管理文件系统命名空间（元数据），DataNode 负责实际的数据块存储。'
        'HDFS 采用一次写入、多次读取（Write-Once-Read-Many）的访问模型，'
        '适用于大规模数据集的批处理场景。', style_body))

    elements.append(Paragraph('1.1 架构总览图', style_h2))
    elements.append(draw_arch_diagram())
    elements.append(Paragraph('图 1：HDFS 核心架构全景图', style_caption))

    elements.append(Paragraph('1.2 核心组件职责', style_h2))
    elements.append(make_table(
        ['组件', '职责', '关键类'],
        [
            ['NameNode', '管理文件系统命名空间，维护文件→块映射，协调数据块副本',
             'NameNode, FSNamesystem, FSDirectory'],
            ['DataNode', '存储实际数据块，执行块创建/删除/复制，向NN发送心跳',
             'DataNode, BPServiceActor, DataXceiver'],
            ['BlockManager', '管理块的副本状态、放置策略、块报告、块重建',
             'BlockManager, DatanodeManager, BlockPlacementPolicy'],
            ['FSEditLog', '编辑日志持久化，记录所有命名空间变更操作',
             'FSEditLog, EditLogOutputStream, JournalManager'],
            ['FSImage', '文件系统镜像（Checkpoint），周期性合并编辑日志',
             'FSImage, FSImageFormat, SaveNamespace'],
            ['HA 子系统', '高可用状态机(Active/Standby/Observer)，自动故障转移',
             'HAState, ActiveState, StandbyState, ZKFC'],
        ],
        col_widths=[30*mm, 75*mm, 65*mm]
    ))

    elements.append(PageBreak())

    # ======================== 二、模块源码结构分析 ========================
    elements.append(Paragraph('二、模块源码结构分析', style_h1))
    elements.append(Paragraph(
        'hadoop-hdfs 模块包含约 716 个 Java 源文件，代码总量超过 30 万行。'
        '以下是各子包的文件分布和核心职责。', style_body))

    elements.append(make_table(
        ['子包', '文件数', '核心职责', '关键大文件'],
        [
            ['server/namenode/', '~210', 'NameNode核心实现', 'FSNamesystem(341KB), NameNodeRpcServer(98KB)'],
            ['server/datanode/', '~131', 'DataNode核心实现', 'DataNode(165KB), DataXceiver(58KB)'],
            ['server/blockmanagement/', '68', '块管理子系统', 'BlockManager(223KB), DatanodeManager(87KB)'],
            ['server/protocol/', '42', '服务端协议定义', 'DatanodeProtocol, NamenodeProtocol'],
            ['protocolPB/', '24', 'ProtoBuf协议翻译层', 'ClientNamenodeProtocolPB 等'],
            ['qjournal/', '33', 'Quorum Journal HA', 'QuorumJournalManager(32KB)'],
            ['server/namenode/ha/', '-', 'HA状态机实现', 'HAState, ActiveState, StandbyState'],
            ['server/namenode/snapshot/', '-', '快照管理', 'SnapshotManager(31KB), DirectorySnapshottableFeature'],
            ['tools/', '17+', '管理工具', 'Balancer, Mover, DFSAdmin'],
            ['util/', '21', '通用工具类', 'DFSUtil, EnumCounters'],
        ],
        col_widths=[38*mm, 15*mm, 42*mm, 75*mm]
    ))
    elements.append(Paragraph('表 1：HDFS 模块子包分布一览', style_caption))

    elements.append(PageBreak())

    # ======================== 三、NameNode 核心架构 ========================
    elements.append(Paragraph('三、NameNode 核心架构', style_h1))

    elements.append(Paragraph('3.1 NameNode 启动流程', style_h2))
    elements.append(Paragraph(
        'NameNode 是 HDFS 的核心元数据管理节点。其启动流程经过精心设计，'
        '确保文件系统命名空间的安全加载和服务的有序启动。'
        '入口方法为 NameNode.main() → createNameNode() → new NameNode(conf) → initialize()。', style_body))

    elements.append(Paragraph('3.1.1 createNameNode() 方法 — 启动选项分发', style_h3))
    elements.append(Paragraph(
        'createNameNode() 解析命令行参数，根据 StartupOption 分发到不同启动路径：', style_body))

    elements.append(make_table(
        ['StartupOption', '操作', '说明'],
        [
            ['FORMAT', 'format(conf, ...)', '格式化文件系统，初始化存储目录'],
            ['GENCLUSTERID', 'NNStorage.newClusterID()', '生成新的集群 ID'],
            ['ROLLBACK', 'doRollback(conf, true)', '回滚到升级前的版本'],
            ['BOOTSTRAPSTANDBY', 'BootstrapStandby.run()', '初始化 Standby NameNode'],
            ['INITIALIZESHAREDEDITS', 'initializeSharedEdits()', '初始化 QJM 共享编辑日志'],
            ['BACKUP/CHECKPOINT', 'new BackupNode()', '创建备份/检查点节点'],
            ['default（正常启动）', 'new NameNode(conf)', '创建 NameNode 实例，进入 initialize()'],
        ],
        col_widths=[40*mm, 55*mm, 75*mm]
    ))

    elements.append(Paragraph('3.1.2 initialize() 方法 — 核心初始化序列', style_h3))
    elements.append(Paragraph(
        'initialize() 是 NameNode 核心初始化流程，按以下严格顺序执行：', style_body))

    init_steps = [
        ['1', 'UserGroupInformation.setConfiguration()', '配置安全认证框架（Kerberos等）'],
        ['2', 'loginAsNameNodeUser()', '以 NameNode 身份登录（Kerberos 认证）'],
        ['3', 'NameNode.initMetrics()', '初始化指标监控系统和 JVM 暂停监控器'],
        ['4', 'startHttpServer()', '启动 HTTP 服务（仅 NAMENODE 角色）'],
        ['5', 'loadNamesystem(conf)', '【关键】加载 FSImage + 重放 EditLog 恢复命名空间'],
        ['6', 'createRpcServer()', '【关键】创建 RPC 服务器（ClientProtocol 等）'],
        ['7', 'startCommonServices()', '【关键】启动 BlockManager、RPC监听、ServicePlugin'],
    ]
    elements.append(make_table(
        ['步骤', '方法调用', '说明'],
        init_steps,
        col_widths=[12*mm, 65*mm, 93*mm]
    ))
    elements.append(Paragraph(
        '其中 startCommonServices() 内部调用 namesystem.startCommonServices() 启动 BlockManager 的块重建线程、'
        '心跳检测线程等，然后调用 rpcServer.start() 开始接受客户端请求。', style_body))

    elements.append(Paragraph('3.2 FSNamesystem — 命名空间核心状态机', style_h2))
    elements.append(Paragraph(
        'FSNamesystem 是 NameNode 的核心类（源码 341KB，约 9200+ 行），'
        '管理文件系统所有命名空间操作，是 HDFS 最大的单文件。它扮演"门面"角色（Facade Pattern），'
        '协调 FSDirectory、BlockManager、FSEditLog、LeaseManager 等子系统。', style_body))
    elements.append(Paragraph(
        '关键设计特征：<br/>'
        '• <b>双重锁机制</b>：先获取 FSNamesystem 写锁（RwLockMode.FS），再获取 dir 写锁<br/>'
        '• <b>操作分类检查</b>：通过 checkOperation(OperationCategory.WRITE/READ) 确保 HA 状态合法<br/>'
        '• <b>安全模式</b>：启动期间只允许读操作，等块报告达到阈值后自动退出<br/>'
        '• <b>编辑日志同步</b>：每次写操作后调用 getEditLog().logSync() 持久化', style_body))

    elements.append(Paragraph('3.3 NameNodeRpcServer — RPC 服务层', style_h2))
    elements.append(Paragraph(
        'NameNodeRpcServer（98KB）实现了多种 Protocol 接口，是客户端和 DataNode 与 NameNode 通信的入口：<br/>'
        '• <b>ClientProtocol</b>：客户端操作接口（create/open/rename/delete/addBlock 等）<br/>'
        '• <b>DatanodeProtocol</b>：DataNode 通信接口（心跳、块报告、命令下发）<br/>'
        '• <b>NamenodeProtocol</b>：NameNode 间通信接口（获取 EditLog、块报告等）<br/>'
        '• <b>HAServiceProtocol</b>：HA 状态管理接口（状态切换、健康检查）', style_body))

    elements.append(PageBreak())

    # ======================== 四、DataNode 核心架构 ========================
    elements.append(Paragraph('四、DataNode 核心架构', style_h1))

    elements.append(Paragraph('4.1 DataNode 组件结构', style_h2))
    elements.append(Paragraph(
        'DataNode（165KB）负责实际的块数据存储和传输。'
        '一个 DataNode 进程可同时服务多个 NameNode（Federation 模式），'
        '通过 BlockPoolManager 管理多个块池。', style_body))

    elements.append(make_table(
        ['组件', '职责', '关键机制'],
        [
            ['BPOfferService', '管理一对NN（主备）的通信', '封装两个 BPServiceActor'],
            ['BPServiceActor', '独立线程，执行心跳/块报告/IBR循环', 'offerService() 主循环'],
            ['DataXceiverServer', '监听数据传输端口，创建 DataXceiver', '每个连接一个线程'],
            ['DataXceiver', '处理单个数据传输请求（读/写/复制）', 'Pipeline 写入、块校验'],
            ['BlockReceiver', '接收数据块并写入本地磁盘', '校验和验证、Pipeline 转发'],
            ['FsDatasetImpl', '管理本地文件系统上的块存储', '多卷管理、磁盘故障检测'],
            ['BlockPoolManager', '管理 DN 参与的所有块池', '支持 Federation 多 NS'],
        ],
        col_widths=[35*mm, 60*mm, 75*mm]
    ))

    elements.append(Paragraph('4.2 心跳与块报告机制 — BPServiceActor.offerService()', style_h2))
    elements.append(Paragraph(
        'offerService() 是 DataNode 与 NameNode 通信的核心循环，运行在独立线程中。'
        '每轮循环按优先级执行以下操作：', style_body))

    elements.append(Paragraph(
        '<b>核心循环流程：</b><br/>'
        '1. <b>发送心跳</b>（默认 3 秒）→ 返回 HeartbeatResponse（HA 状态、命令列表、块报告租约）<br/>'
        '2. <b>更新 HA 状态</b> → 如 STANDBY→ACTIVE，确保在处理命令前完成状态切换<br/>'
        '3. <b>处理 NN 命令</b> → KeyUpdateCommand 优先入队（enqueueFirst），其他命令普通入队<br/>'
        '4. <b>发送增量块报告（IBR）</b> → 报告自上次以来的块变更<br/>'
        '5. <b>发送全量块报告</b>（默认 6 小时）→ 使用租约机制防止并发<br/>'
        '6. <b>发送缓存报告</b> → 报告 DN 缓存的数据块状态<br/>'
        '7. <b>等待下一轮</b> → ibrManager.waitTillNextIBR(heartbeatWaitTime)', style_body))

    elements.append(Paragraph('4.3 DataXceiver.writeBlock() — Pipeline 写入', style_h2))
    elements.append(Paragraph(
        'writeBlock() 实现了 HDFS 数据写入 Pipeline 的核心逻辑，流程如下：<br/>'
        '1. 确定角色（isClient / isDatanode / isTransfer）和权限检查<br/>'
        '2. 创建 BlockReceiver 打开本地写入器<br/>'
        '3. 与下游 DataNode 建立 Socket 连接（含 SASL 安全握手）<br/>'
        '4. 转发 writeBlock 请求到下游节点<br/>'
        '5. 读取下游连接应答，向上游发送应答<br/>'
        '6. 通过 blockReceiver.receiveBlock() 接收数据并转发<br/>'
        '7. 收尾处理（更新 GenerationStamp、关闭块、更新指标）', style_body))

    elements.append(PageBreak())

    # ======================== 五、BlockManager 块管理 ========================
    elements.append(Paragraph('五、BlockManager 块管理', style_h1))

    elements.append(Paragraph('5.1 BlockManager 概览', style_h2))
    elements.append(Paragraph(
        'BlockManager（223KB，约 5800+ 行）是 NameNode 中最大的组件之一，'
        '负责管理所有数据块的生命周期。核心职责包括：', style_body))
    elements.append(Paragraph(
        '• 块→DataNode 映射管理（blocksMap）<br/>'
        '• 块副本放置策略（BlockPlacementPolicy）<br/>'
        '• 块报告处理（processReport）<br/>'
        '• 块重建（ReplicationMonitor / RedundancyMonitor）<br/>'
        '• 块失效管理（InvalidateBlocks）<br/>'
        '• 心跳管理（HeartbeatManager）', style_body))

    elements.append(Paragraph('5.2 副本放置策略 — BlockPlacementPolicyDefault', style_h2))
    elements.append(Paragraph(
        'chooseTargetInOrder() 实现了经典的三副本放置算法，这是 HDFS 高可用性和性能的关键设计：', style_body))

    elements.append(make_table(
        ['副本', '放置位置', '选择方法', '设计考量'],
        [
            ['第1副本', '本地节点（Writer 所在）', 'chooseLocalStorage()', '减少网络传输'],
            ['第2副本', '远程机架上的一个节点', 'chooseRemoteRack()', '容忍机架故障'],
            ['第3副本', '第2副本同机架的不同节点', 'chooseLocalRack(dn1)', '平衡可用性与带宽'],
            ['更多副本', '随机选择', 'chooseRandom()', '负载均衡'],
        ],
        col_widths=[20*mm, 40*mm, 45*mm, 65*mm]
    ))
    elements.append(Paragraph(
        '<b>容错重试机制</b>：当节点不足时，第一次重试允许选择陈旧节点（avoidStaleNodes=false），'
        '第二次重试放宽存储类型约束。最终节点通过 getPipeline() 排序形成写入管道。', style_body))

    elements.append(Paragraph('5.3 块报告处理 — processReport()', style_h2))
    elements.append(Paragraph(
        'processReport() 处理 DataNode 发送的全量块报告，核心采用差异化处理策略：<br/>'
        '<b>首次报告</b>（processFirstBlockReport）：高效处理，用于加速 NameNode 重启<br/>'
        '<b>后续报告</b>（processReport 内部）：通过 reportDiff() 计算五个差异队列：', style_body))

    elements.append(make_table(
        ['队列', '含义', '处理方法'],
        [
            ['toAdd', '新增的块（DN有，NN无）', 'addStoredBlock()'],
            ['toRemove', '需移除的块', 'removeStoredBlock()'],
            ['toInvalidate', '需失效的块（NN有，DN不应有）', 'addToInvalidates()'],
            ['toCorrupt', '损坏的块', 'markBlockAsCorrupt()'],
            ['toUC', '正在构建的块（Under Construction）', 'addStoredBlockUnderConstruction()'],
        ],
        col_widths=[30*mm, 60*mm, 80*mm]
    ))
    elements.append(Paragraph(
        '注意：processReport() 持有全局写锁（RwLockMode.GLOBAL），'
        '在安全模式下会丢弃非首次块报告以减少启动时间。', style_body))

    elements.append(PageBreak())

    # ======================== 六、HA 高可用机制 ========================
    elements.append(Paragraph('六、HA 高可用机制（状态模式）', style_h1))

    elements.append(Paragraph('6.1 状态模式实现', style_h2))
    elements.append(Paragraph(
        'HDFS HA 采用经典的状态模式（State Pattern）实现，由抽象基类 HAState 和三个具体状态类组成：', style_body))

    elements.append(draw_ha_state_diagram())
    elements.append(Paragraph('图 2：HA 状态机转换图', style_caption))

    elements.append(Paragraph('6.2 HAState 核心方法 — setStateInternal()', style_h2))
    elements.append(Paragraph(
        'setStateInternal() 是状态转换的模板方法（Template Method Pattern），按固定步骤执行：', style_body))

    elements.append(make_table(
        ['步骤', '方法', '说明', '持锁状态'],
        [
            ['①', 'prepareToExitState()', '准备退出当前状态（如取消 checkpoint）', '无锁'],
            ['②', 's.prepareToEnterState()', '准备进入新状态（前置条件检查）', '无锁'],
            ['③', 'context.writeLock()', '获取写锁', '加锁'],
            ['④', 'exitState()', '退出当前状态（停止服务）', '持锁'],
            ['⑤', 'context.setState(s)', '设置新状态', '持锁'],
            ['⑥', 's.enterState()', '进入新状态（启动服务）', '持锁'],
            ['⑦', 'updateLastHATransitionTime()', '记录转换时间戳', '持锁'],
        ],
        col_widths=[12*mm, 50*mm, 65*mm, 20*mm]
    ))

    elements.append(Paragraph('6.3 三种状态的行为差异', style_h2))
    elements.append(make_table(
        ['特性', 'ActiveState', 'StandbyState', 'ObserverState'],
        [
            ['允许的操作', 'READ + WRITE', 'UNCHECKED + Stale READ', 'READ（重定向WRITE到Active）'],
            ['填充副本队列', '是', '否', '否'],
            ['可转换到', 'Standby', 'Active / Observer', 'Standby'],
            ['enterState()', 'startActiveServices()', 'startStandbyServices()', 'startStandbyServices()'],
            ['exitState()', 'stopActiveServices()', 'stopStandbyServices()', 'stopStandbyServices()'],
        ],
        col_widths=[30*mm, 40*mm, 45*mm, 55*mm]
    ))

    elements.append(Paragraph('6.4 Quorum Journal Manager (QJM)', style_h2))
    elements.append(Paragraph(
        'QuorumJournalManager（32KB）是 HA 共享编辑日志的核心实现，通过写入远程 JournalNode 集群实现编辑日志的高可用：<br/>'
        '• Active NameNode 将编辑日志写入多数 JournalNode（多数派确认）<br/>'
        '• Standby NameNode 通过 EditLogTailer 定期拉取编辑日志<br/>'
        '• StandbyCheckpointer 周期性合并 FSImage + EditLog，减少重启时间<br/>'
        '• Fencing 机制确保同一时刻只有一个 Active NameNode 可写入', style_body))

    elements.append(PageBreak())

    # ======================== 七、INode 文件系统树 ========================
    elements.append(Paragraph('七、文件系统树（INode 层次结构）', style_h1))

    elements.append(Paragraph('7.1 INode 类继承体系', style_h2))
    elements.append(draw_inode_hierarchy())
    elements.append(Paragraph('图 3：INode 类继承层次图', style_caption))

    elements.append(Paragraph(
        'HDFS 文件系统树由 INode 层次结构在内存中维护，所有文件和目录操作都通过 FSDirectory 在此树上执行：<br/>'
        '• <b>INode</b>：抽象基类，定义文件/目录的通用接口<br/>'
        '• <b>INodeWithAdditionalFields</b>：添加 id、name、permission（64位编码 user+group+mode）、时间戳<br/>'
        '• <b>INodeFile</b>：文件节点，header 字段使用 64 位编码存储 StoragePolicy(4bit) + 副本/EC策略(12bit) + blockSize(48bit)<br/>'
        '• <b>INodeDirectory</b>：目录节点，children 列表默认初始容量仅为 2（性能分析的经验值）<br/>'
        '• <b>INodeReference</b>：快照引用节点，支持快照中的文件/目录引用', style_body))

    elements.append(Paragraph('7.2 快照感知操作', style_h2))
    elements.append(Paragraph(
        'INodeDirectory 的所有子节点操作（getChild/addChild/removeChild/getChildrenList）都是"快照感知"的：<br/>'
        '• 当 snapshotId == CURRENT_STATE_ID 时，操作当前视图（getCurrentChildrenList）<br/>'
        '• 当访问快照视图时，通过 DirectoryWithSnapshotFeature 的 DiffList 查找历史状态<br/>'
        '• 修改操作（addChild/removeChild）在快照存在时会记录 diff，实现写时复制（Copy-on-Write）语义<br/>'
        '• SnapshotManager 全局管理快照 ID 计数器和数量限制', style_body))

    elements.append(PageBreak())

    # ======================== 八、编辑日志与持久化 ========================
    elements.append(Paragraph('八、编辑日志与持久化机制', style_h1))

    elements.append(Paragraph('8.1 FSEditLog 双缓冲区同步机制', style_h2))
    elements.append(draw_editlog_buffer())
    elements.append(Paragraph('图 4：FSEditLog 双缓冲区同步机制', style_caption))

    elements.append(Paragraph('8.2 logEdit() — 编辑日志写入', style_h2))
    elements.append(Paragraph(
        'logEdit() 是编辑日志写入的入口方法，关键流程：<br/>'
        '1. <b>synchronized 块内</b>：waitIfAutoSyncScheduled() → beginTransaction(op) → doEditTransaction(op)<br/>'
        '2. doEditTransaction() 将操作写入缓冲区（editLogStream.write(op)），检查是否需要强制同步<br/>'
        '3. 如需同步，在 <b>synchronized 块外</b>调用 logSync()，允许其他线程继续写入', style_body))

    elements.append(Paragraph('8.3 logSync() — 批量同步与双缓冲区交换', style_h2))
    elements.append(Paragraph(
        'logSync() 是编辑日志持久化的核心，实现了高效的批量同步机制：', style_body))

    elements.append(make_table(
        ['阶段', '操作', '持锁状态', '说明'],
        [
            ['等待', 'while (mytxid > synctxid && isSyncRunning) wait()', 'synchronized', '等待其他线程同步完成'],
            ['检查', 'if (mytxid <= synctxid) return', 'synchronized', '事务已被批量同步'],
            ['交换', 'editLogStream.setReadyToFlush()', 'synchronized', '交换双缓冲区'],
            ['释放锁', 'doneWithAutoSyncScheduling()', 'synchronized', '通知等待的写线程'],
            ['刷盘', 'logStream.flush()', '无锁', '实际持久化（耗时操作）'],
            ['更新', 'synctxid = lastJournalledTxId; notifyAll()', 'synchronized', '更新已同步ID并唤醒'],
        ],
        col_widths=[18*mm, 60*mm, 25*mm, 67*mm]
    ))
    elements.append(Paragraph(
        '<b>关键设计要点</b>：<br/>'
        '• 双缓冲区机制让写入和刷盘可以并行进行，极大提升吞吐量<br/>'
        '• 批量同步机制使多个线程的编辑被一次 flush() 一起持久化<br/>'
        '• 同步失败时 NameNode 立即调用 terminate(1) 退出（Fail-Fast 策略），防止数据不一致', style_body))

    elements.append(Paragraph('8.4 FSImage — 文件系统镜像', style_h2))
    elements.append(Paragraph(
        'FSImage（60KB）负责文件系统镜像的持久化，核心机制：<br/>'
        '• <b>Checkpoint</b>：将 FSImage + EditLog 合并为新的 FSImage<br/>'
        '• <b>SaveNamespace</b>：将内存中的命名空间序列化到磁盘<br/>'
        '• <b>加载流程</b>：启动时先加载最新的 FSImage，再重放后续的 EditLog 事务<br/>'
        '• <b>Standby Checkpointer</b>：Standby NN 定期执行 Checkpoint 并上传到 Active NN', style_body))

    elements.append(PageBreak())

    # ======================== 九、核心流程时序分析 ========================
    elements.append(Paragraph('九、核心流程时序分析', style_h1))

    elements.append(Paragraph('9.1 文件写入完整流程', style_h2))
    elements.append(draw_write_pipeline())
    elements.append(Paragraph('图 5：数据写入 Pipeline 时序图', style_caption))

    elements.append(Paragraph(
        '<b>文件写入详细流程（startFileInt 方法分析）</b>：<br/>'
        '1. 路径校验：DFSUtil.isValidName(src) + 保留名检查<br/>'
        '2. 副本策略决策：SHOULD_REPLICATE → verifyReplication() / 否则检查 EC 策略<br/>'
        '3. 获取写锁 writeLock(RwLockMode.FS)<br/>'
        '4. checkOperation(WRITE) + checkNameNodeSafeMode() 确保 Active 且非安全模式<br/>'
        '5. FSDirWriteFileOp.resolvePathForStartFile() 解析路径<br/>'
        '6. 处理加密区域（释放锁→生成 EDEK→重新获取锁→重新解析路径）<br/>'
        '7. dir.writeLock() → FSDirWriteFileOp.startFile() 创建文件 INode<br/>'
        '8. 释放锁后 getEditLog().logSync() 持久化编辑日志', style_body))

    elements.append(Paragraph('9.2 心跳处理流程', style_h2))
    elements.append(Paragraph(
        '<b>DataNode 心跳主循环（BPServiceActor.offerService）</b>：<br/>'
        '每轮循环完整流程：<br/>'
        '① 判断心跳是否到期 → ② 发送心跳并获取响应 → ③ 更新HA状态（先于命令处理）<br/>'
        '→ ④ 处理NN下发命令（KeyUpdate优先入队）→ ⑤ 发送IBR增量块报告<br/>'
        '→ ⑥ 发送全量块报告（使用租约）→ ⑦ 发送缓存报告 → ⑧ 等待下一轮', style_body))

    elements.append(Paragraph(
        '<b>NameNode 命令类型（DatanodeProtocol.DNA_* 常量）</b>：<br/>'
        '• DNA_TRANSFER = 1：传输块到其他 DataNode<br/>'
        '• DNA_INVALIDATE = 2：删除指定块<br/>'
        '• DNA_SHUTDOWN = 3：关闭 DataNode<br/>'
        '• DNA_REGISTER = 4：要求重新注册<br/>'
        '• DNA_FINALIZE = 5：最终化升级<br/>'
        '• DNA_RECOVERBLOCK = 6：块恢复<br/>'
        '• DNA_BALANCERBANDWIDTHUPDATE = 10：更新均衡器带宽<br/>'
        '• DNA_CACHE = 11 / DNA_UNCACHE = 12：缓存/取消缓存块<br/>'
        '• DNA_ERASURE_CODING_RECONSTRUCTION = 13：纠删码重建', style_body))

    elements.append(PageBreak())

    # ======================== 十、设计模式总结 ========================
    elements.append(Paragraph('十、设计模式总结', style_h1))

    elements.append(make_table(
        ['设计模式', '应用场景', '关键类', '设计意图'],
        [
            ['状态模式 (State)', 'HA 状态管理', 'HAState, ActiveState, StandbyState',
             '将状态特定行为封装在独立类中，状态转换清晰可扩展'],
            ['模板方法 (Template Method)', 'HA 状态转换', 'HAState.setStateInternal()',
             '定义转换骨架（prepareExit→prepareEnter→exit→enter），子类实现细节'],
            ['策略模式 (Strategy)', '块副本放置', 'BlockPlacementPolicy, Default/Rack-Aware',
             '算法可插拔，支持自定义副本放置策略'],
            ['门面模式 (Facade)', 'NameNode 核心', 'FSNamesystem',
             '统一入口协调 FSDirectory/BlockManager/FSEditLog/LeaseManager'],
            ['观察者模式 (Observer)', '编辑日志', 'JournalSet + 多个 JournalManager',
             '一次写入操作广播到所有 Journal（File + QJM）'],
            ['工厂模式 (Factory)', 'RPC 协议', 'ProtocolPB 翻译层',
             'ProtoBuf 序列化与服务端接口的解耦'],
            ['组合模式 (Composite)', '文件系统树', 'INode / INodeFile / INodeDirectory',
             '统一的文件/目录树操作接口'],
            ['装饰器模式 (Decorator)', 'INode 特性', 'Feature 接口 (AclFeature, XAttrFeature等)',
             '通过 Feature[] 动态扩展 INode 功能'],
            ['双缓冲区', '编辑日志同步', 'FSEditLog logSync/setReadyToFlush',
             '写入和刷盘并行，提升吞吐量'],
            ['租约模式 (Lease)', '文件写入控制', 'LeaseManager / BlockReportLeaseManager',
             '通过租约控制并发写入和块报告'],
        ],
        col_widths=[30*mm, 30*mm, 55*mm, 55*mm]
    ))
    elements.append(Paragraph('表 2：HDFS 核心设计模式一览', style_caption))

    elements.append(PageBreak())

    # ======================== 十一、关键场景调用链 ========================
    elements.append(Paragraph('十一、关键场景调用链', style_h1))

    elements.append(Paragraph('11.1 文件创建调用链', style_h2))
    elements.append(Paragraph(
        'Client.create() → DFSClient.create() → NameNodeRpcServer.create()'
        ' → FSNamesystem.startFile() → startFileInt()'
        ' → FSDirWriteFileOp.resolvePathForStartFile()'
        ' → FSDirWriteFileOp.startFile()'
        ' → FSEditLog.logOpenFile() → logSync()', style_code))

    elements.append(Paragraph('11.2 块分配调用链', style_h2))
    elements.append(Paragraph(
        'Client.addBlock() → NameNodeRpcServer.addBlock()'
        ' → FSNamesystem.getAdditionalBlock()'
        ' → BlockManager.chooseTarget4NewBlock()'
        ' → BlockPlacementPolicyDefault.chooseTarget()'
        ' → chooseTargetInOrder()'
        ' → chooseLocalStorage() / chooseRemoteRack() / chooseLocalRack()', style_code))

    elements.append(Paragraph('11.3 心跳处理调用链', style_h2))
    elements.append(Paragraph(
        'BPServiceActor.offerService()'
        ' → sendHeartBeat()'
        ' → DatanodeProtocol.sendHeartbeat()'
        ' → NameNodeRpcServer.sendHeartbeat()'
        ' → FSNamesystem.handleHeartbeat()'
        ' → BlockManager.DatanodeManager.handleHeartbeat()'
        ' → HeartbeatManager.updateHeartbeat()', style_code))

    elements.append(Paragraph('11.4 块报告处理调用链', style_h2))
    elements.append(Paragraph(
        'BPServiceActor.blockReport()'
        ' → DatanodeProtocol.blockReport()'
        ' → NameNodeRpcServer.blockReport()'
        ' → BlockManager.processReport()'
        ' → processFirstBlockReport() 或 processReport(内部)'
        ' → reportDiff() → addStoredBlock() / removeStoredBlock()', style_code))

    elements.append(Paragraph('11.5 HA Failover 调用链', style_h2))
    elements.append(Paragraph(
        'ZKFC.fenceOldActive() / becomeActive()'
        ' → HAServiceProtocol.transitionToActive()'
        ' → NameNode.transitionToActive()'
        ' → HAState.setState(ACTIVE_STATE)'
        ' → ActiveState.setStateInternal()'
        ' → StandbyState.prepareToExitState()'
        ' → ActiveState.prepareToEnterState()'
        ' → StandbyState.exitState() → stopStandbyServices()'
        ' → ActiveState.enterState() → startActiveServices()', style_code))

    elements.append(Paragraph('11.6 快照创建调用链', style_h2))
    elements.append(Paragraph(
        'Client.createSnapshot()'
        ' → NameNodeRpcServer.createSnapshot()'
        ' → FSNamesystem.createSnapshot()'
        ' → SnapshotManager.createSnapshot()'
        ' → INodeDirectory.addSnapshot()'
        ' → DirectorySnapshottableFeature.addSnapshot()'
        ' → 更新 snapshotCounter / numSnapshots', style_code))

    elements.append(Paragraph('11.7 Pipeline 数据写入调用链', style_h2))
    elements.append(Paragraph(
        'Client.DFSOutputStream.writeChunk()'
        ' → DataStreamer.run() → createBlockOutputStream()'
        ' → DataXceiver.writeBlock()'
        ' → getBlockReceiver() → 创建 BlockReceiver'
        ' → 建立到下游 DN 的 Socket 连接'
        ' → new Sender(mirrorOut).writeBlock() → 转发到下游'
        ' → blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut)', style_code))

    elements.append(PageBreak())

    # ======================== 十二、核心源文件清单 ========================
    elements.append(Paragraph('十二、核心源文件清单', style_h1))

    core_files = [
        ['NameNode.java', '110KB', 'HDFS 主入口点，HA 状态管理'],
        ['FSNamesystem.java', '341KB', '命名空间核心状态机（模块最大文件）'],
        ['FSDirectory.java', '75KB', '内存目录树管理（INode 层次）'],
        ['FSEditLog.java', '64KB', '编辑日志管理（双缓冲区同步）'],
        ['FSImage.java', '60KB', '文件系统镜像持久化'],
        ['NameNodeRpcServer.java', '98KB', 'NameNode RPC 服务端'],
        ['DataNode.java', '165KB', 'DataNode 核心实现'],
        ['BPServiceActor.java', '53KB', 'DataNode 心跳/块报告线程'],
        ['DataXceiver.java', '58KB', '数据传输处理器（Pipeline 实现）'],
        ['BlockReceiver.java', '67KB', '块数据接收与写盘'],
        ['BlockManager.java', '223KB', '块管理核心（第二大文件）'],
        ['BlockPlacementPolicyDefault.java', '56KB', '默认副本放置策略'],
        ['DatanodeManager.java', '87KB', 'DataNode 管理器'],
        ['HeartbeatManager.java', '19KB', '心跳管理与过期检测'],
        ['HAState.java', '5KB', 'HA 状态机基类（状态模式）'],
        ['ActiveState.java', '2.4KB', 'Active 状态实现'],
        ['StandbyState.java', '4KB', 'Standby/Observer 状态实现'],
        ['QuorumJournalManager.java', '32KB', 'QJM 共享编辑日志客户端'],
        ['INode.java', '39KB', '文件/目录抽象基类'],
        ['INodeFile.java', '42KB', '文件 INode（64位 header 编码）'],
        ['INodeDirectory.java', '35KB', '目录 INode（快照感知操作）'],
        ['LeaseManager.java', '22KB', '文件租约管理'],
        ['SnapshotManager.java', '31KB', '快照管理器'],
        ['DatanodeProtocol.java', '9KB', 'DN↔NN 通信协议接口'],
    ]
    elements.append(make_table(
        ['源文件', '大小', '核心职责'],
        core_files,
        col_widths=[55*mm, 18*mm, 97*mm]
    ))
    elements.append(Paragraph('表 3：HDFS 核心源文件清单（24 个关键文件）', style_caption))

    return elements

# ============================================================
# 主函数
# ============================================================
def main():
    output_dir = '/Users/meiyangchen/CodeBuddy/20260324152704'
    output_file = os.path.join(output_dir, 'Hadoop_HDFS_release-3.3.5-RC0_核心架构设计文档.pdf')

    doc = SimpleDocTemplate(
        output_file,
        pagesize=A4,
        leftMargin=20*mm,
        rightMargin=20*mm,
        topMargin=20*mm,
        bottomMargin=20*mm,
        title='Hadoop HDFS 核心架构设计文档',
        author='Architecture Analysis Tool',
    )

    elements = build_content()
    doc.build(elements, onFirstPage=on_first_page, onLaterPages=on_later_pages)
    print(f'✅ PDF 文档已生成: {output_file}')
    print(f'   文件大小: {os.path.getsize(output_file) / 1024:.1f} KB')

if __name__ == '__main__':
    main()
