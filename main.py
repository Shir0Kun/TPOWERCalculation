from fastapi import FastAPI, UploadFile, File, Response
import io
import json
import traceback
import pandas as pd
from openpyxl import load_workbook
from collections import Counter
from typing import Dict, List
import tempfile
import os
from datetime import datetime
import re

app = FastAPI()

def process_excel_data(contents):
    """处理Excel数据的通用函数"""
    excel_bytes = io.BytesIO(contents)
    wb = load_workbook(excel_bytes, data_only=True)
    sheet = wb.active

    # 读取所有数据
    max_row = sheet.max_row
    max_col = sheet.max_column
    all_data = []
    
    for row_idx in range(1, max_row + 1):
        row_data = []
        for col_idx in range(1, max_col + 1):
            cell_value = sheet.cell(row=row_idx, column=col_idx).value
            row_data.append(cell_value)
        
        if any(cell is not None and str(cell).strip() != "" for cell in row_data):
            all_data.append(row_data)
    
    if not all_data:
        return None, "Excel文件为空"

    # 检测标题行
    header_row_index = None
    for i, row in enumerate(all_data):
        row_text = " ".join(str(cell) for cell in row if cell)
        if any(keyword in row_text for keyword in ["代理", "ID", "金额", "BONUS", "上分"]):
            header_row_index = i
            break

    if header_row_index is None:
        header_row_index = 0

    # 创建DataFrame
    headers = [str(c).strip() if c else f"Column_{i}" for i, c in enumerate(all_data[header_row_index])]
    data_rows = all_data[header_row_index + 1:]
    
    df = pd.DataFrame(data_rows, columns=headers)
    df = df.dropna(how="all")
    df = df.reset_index(drop=True)
    df = df.where(pd.notnull(df), None)
    
    return df, None

def extract_agent_number(agent_str):
    """从代理序号中提取数字，处理各种格式"""
    if not agent_str:
        return 0
    
    agent_str = str(agent_str).strip()
    
    # 尝试提取数字
    numbers = re.findall(r'\d+', agent_str)
    if numbers:
        return int(numbers[0])
    else:
        return 0

def sort_by_agent_number(df):
    """按代理序号排序：先按上分地方，再按代理序号"""
    if '上分地方' in df.columns and '代理序号' in df.columns:
        # 创建排序键：先按地点，再按代理序号类型，最后按数字
        def sort_key(row):
            location = str(row['上分地方']) if row['上分地方'] is not None else ""
            agent = str(row['代理序号']) if row['代理序号'] is not None else ""
            
            # 地点排序
            location_order = {
                "BELLA": 1,
                "肥子代理 638": 2,
                "OC619-01-01": 3,
                "WS": 4,
                "OC619-01-01-01": 5
            }
            
            # 代理序号排序
            agent_upper = agent.upper()
            if "IN" in agent_upper:
                agent_type = 1  # IN类型在前
            elif "OUT" in agent_upper:
                agent_type = 2  # OUT类型在后
            else:
                agent_type = 3  # 其他类型在最后
            
            # 提取数字
            agent_number = extract_agent_number(agent)
            
            return (location_order.get(location, 99), agent_type, agent_number, location, agent)
        
        # 应用排序
        df_sorted = df.copy()
        df_sorted['sort_key'] = df_sorted.apply(sort_key, axis=1)
        df_sorted = df_sorted.sort_values('sort_key')
        df_sorted = df_sorted.drop('sort_key', axis=1)
        return df_sorted.reset_index(drop=True)
    else:
        # 如果缺少列，就按原始顺序
        return df

def calculate_oc619_commission(locations_data):
    """计算OC619系列的多层佣金分配 - 修正版"""
    # 定义层级关系
    hierarchy = {
        # 第三层 (底层)
        "OC619-01-01": {"level": 3, "parent": "OC619-01"},
        "OC619-01-02": {"level": 3, "parent": "OC619-01"},
        "OC619-01-03": {"level": 3, "parent": "OC619-01"},
        "OC619-01-01-01": {"level": 3, "parent": "OC619-01-01"},
        
        # 第二层
        "OC619-01": {"level": 2, "parent": "OC619"},
        
        # 第一层 (顶层)
        "OC619": {"level": 1, "parent": None}
    }
    
    # 初始化各层总金额
    level_totals = {
        1: {},  # 第一层
        2: {},  # 第二层  
        3: {}   # 第三层
    }
    
    # 计算每个地点的直接业绩
    direct_sales = {}
    for location, data in locations_data.items():
        if location in hierarchy:
            direct_sales[location] = data["total_amount"]
    
    # 计算第三层业绩 (底层) - 每个地点的直接业绩
    for location, info in hierarchy.items():
        if info["level"] == 3:
            level_totals[3][location] = direct_sales.get(location, 0)
    
    # 计算第二层业绩 (包含下属第三层)
    for location, info in hierarchy.items():
        if info["level"] == 2:
            # 第二层总业绩 = 自己直接业绩 + 所有下属第三层业绩
            total = direct_sales.get(location, 0)
            # 找到所有下属第三层
            for sub_location, sub_info in hierarchy.items():
                if sub_info["level"] == 3 and sub_info["parent"] == location:
                    total += level_totals[3].get(sub_location, 0)
            level_totals[2][location] = total
    
    # 计算第一层业绩 (包含下属所有层)
    for location, info in hierarchy.items():
        if info["level"] == 1:
            # 第一层总业绩 = 自己直接业绩 + 所有下属第二层总业绩
            total = direct_sales.get(location, 0)
            for sub_location, sub_info in hierarchy.items():
                if sub_info["level"] == 2 and sub_info["parent"] == location:
                    total += level_totals[2].get(sub_location, 0)
            level_totals[1][location] = total
    
    # 计算佣金 - 修正版
    commission_breakdown = {}
    
    # 第三层佣金: 全部第三层业绩总和 × 5%，然后平分给有业绩的第三层
    total_level3_sales = sum(level_totals[3].values())
    total_level3_commission = total_level3_sales * 0.05
    
    # 计算有业绩的第三层数量
    active_level3_locations = [loc for loc, sales in level_totals[3].items() if sales > 0]
    
    if active_level3_locations and total_level3_commission > 0:
        # 平分给有业绩的第三层
        commission_per_location = total_level3_commission / len(active_level3_locations)
        for location in active_level3_locations:
            commission_breakdown[location] = commission_per_location
    
    # 第二层佣金: 自己及下属总业绩 × 20% - 下属第三层佣金
    for location in level_totals[2]:
        if level_totals[2][location] > 0:
            base_commission = level_totals[2][location] * 0.20
            
            # 减去下属第三层佣金
            subordinate_commission = 0
            for sub_location, sub_info in hierarchy.items():
                if sub_info["level"] == 3 and sub_info["parent"] == location:
                    subordinate_commission += commission_breakdown.get(sub_location, 0)
            
            final_commission = base_commission - subordinate_commission
            if final_commission > 0:
                commission_breakdown[location] = final_commission
    
    # 第一层佣金: 整个团队总业绩 × 5% - 下属所有佣金
    for location in level_totals[1]:
        if level_totals[1][location] > 0:
            base_commission = level_totals[1][location] * 0.05
            
            # 减去下属所有佣金
            subordinate_commission = 0
            for sub_location, sub_info in hierarchy.items():
                if sub_info["parent"] == location:
                    subordinate_commission += commission_breakdown.get(sub_location, 0)
            
            final_commission = base_commission - subordinate_commission
            if final_commission > 0:
                commission_breakdown[location] = final_commission
    
    return commission_breakdown, level_totals, total_level3_sales

def calculate_commission(locations_data):
    """计算所有地点的佣金分配"""
    commission_results = {}
    
    # 分离OC619系列和其他代理
    oc619_locations = {}
    other_locations = {}
    
    for location, data in locations_data.items():
        if "OC619" in str(location):
            oc619_locations[location] = data
        else:
            other_locations[location] = data
    
    # 计算OC619系列佣金
    if oc619_locations:
        oc619_commission, oc619_totals, total_level3_sales = calculate_oc619_commission(oc619_locations)
        commission_results.update(oc619_commission)
    
    # 计算其他代理佣金 (30%)
    for location, data in other_locations.items():
        total_amount = data["total_amount"]
        if total_amount > 0:
            commission_results[location] = total_amount * 0.30
    
    return commission_results

@app.post("/upload-excel/")
async def upload_excel(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df, error = process_excel_data(contents)
        
        if error:
            return {"error": error}

        # 按上分地方分组统计
        location_stats = {}
        location_details = {}
        
        for index, row in df.iterrows():
            location = str(row.get("上分地方", "") or "").strip()
            if not location:
                location = "未知地点"
            
            # 统计每个地点的交易
            if location not in location_stats:
                location_stats[location] = 0
                location_details[location] = []
            
            location_stats[location] += 1
            
            # 记录详细信息
            transaction_info = {
                "代理序号": row.get("代理序号"),
                "ID": row.get("ID"),
                "金额": row.get("金额"),
                "BONUS": row.get("BONUS"),
                "行号": index + 1
            }
            location_details[location].append(transaction_info)

        # 计算每个地点的金额统计
        location_amounts = {}
        locations_data_for_commission = {}
        
        for location, transactions in location_details.items():
            amounts = [t["金额"] for t in transactions if t["金额"] is not None]
            in_transactions = [t for t in transactions if "IN" in str(t["代理序号"]).upper()]
            out_transactions = [t for t in transactions if "OUT" in str(t["代理序号"]).upper()]
            
            if amounts:
                total_amount = sum(amounts)
                
                location_amounts[location] = {
                    "总交易笔数": len(transactions),
                    "IN笔数": len(in_transactions),
                    "OUT笔数": len(out_transactions),
                    "总金额": total_amount,
                    "平均金额": total_amount / len(amounts),
                    "最大金额": max(amounts),
                    "最小金额": min(amounts)
                }
                
                locations_data_for_commission[location] = {
                    "total_amount": total_amount,
                    "transaction_count": len(transactions)
                }

        # 计算佣金
        commission_results = calculate_commission(locations_data_for_commission)

        # 预览数据（排序后的）
        df_sorted = sort_by_agent_number(df)
        preview_data = []
        for i in range(min(10, len(df_sorted))):
            row_data = {col: df_sorted.iloc[i][col] for col in df_sorted.columns}
            preview_data.append(row_data)

        return {
            "success": True,
            "total_rows": len(df),
            "location_summary": {
                "by_location": location_stats,
                "amount_analysis": location_amounts,
                "commission_calculation": commission_results,
                "total_locations": len(location_stats)
            },
            "preview": preview_data,
            "export_ready": True,
            "sorting_applied": "按上分地方和代理序号排序"
        }

    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.post("/export-sorted/")
async def export_sorted(file: UploadFile = File(...)):
    """完全排序导出 - 包含修正的多层佣金计算"""
    try:
        contents = await file.read()
        df, error = process_excel_data(contents)
        
        if error:
            return {"error": error}

        # 完全排序：先按上分地方，再按代理序号
        df_sorted = sort_by_agent_number(df)
        
        # 准备佣金计算数据
        locations_data = {}
        locations = df_sorted['上分地方'].dropna().unique()
        
        for location in locations:
            if location:
                location_data = df_sorted[df_sorted['上分地方'] == location]
                total_amount = location_data['金额'].sum()
                locations_data[location] = {
                    "total_amount": total_amount,
                    "transaction_count": len(location_data)
                }
        
        # 计算佣金
        commission_results = calculate_commission(locations_data)
        
        # 创建详细的佣金计算表
        commission_details = []
        for location in locations:
            if location:
                location_data = df_sorted[df_sorted['上分地方'] == location]
                total_amount = location_data['金额'].sum()
                
                commission_row = {
                    '上分地方': location,
                    '总金额': total_amount,
                    '交易笔数': len(location_data)
                }
                
                # 添加佣金信息
                if location in commission_results:
                    commission_row['佣金'] = commission_results[location]
                    if "OC619" in location:
                        commission_row['佣金率'] = "多层分配"
                    else:
                        commission_row['佣金率'] = "30%"
                else:
                    commission_row['佣金'] = 0
                    commission_row['佣金率'] = "0%"
                
                commission_details.append(commission_row)
        
        # 导出
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet 1: 排序后的数据
            df_sorted.to_excel(writer, sheet_name='Sorted_Data', index=False)
            
            # Sheet 2: 地点统计
            location_summary = df_sorted.groupby('上分地方').agg({
                '代理序号': 'count',
                '金额': ['sum', 'mean', 'min', 'max']
            }).round(2)
            location_summary.columns = ['交易笔数', '总金额', '平均金额', '最小金额', '最大金额']
            location_summary.to_excel(writer, sheet_name='Location_Statistics')
            
            # Sheet 3: 佣金计算详情
            commission_df = pd.DataFrame(commission_details)
            commission_df.to_excel(writer, sheet_name='Commission_Details', index=False)
            
            # Sheet 4: 佣金汇总
            summary_data = []
            total_all_commission = 0
            for agent, commission in commission_results.items():
                summary_data.append({
                    '代理/层级': agent,
                    '佣金金额': commission
                })
                total_all_commission += commission
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.loc[len(summary_df)] = {'代理/层级': '总计', '佣金金额': total_all_commission}
            summary_df.to_excel(writer, sheet_name='Commission_Summary', index=False)
            
            # Sheet 5: 修正的佣金规则说明
            rules_df = pd.DataFrame({
                'OC619多层佣金规则（修正版）': [
                    '第一层 (OC619):',
                    '  - 计算: (自己业绩 + 下属所有层业绩) × 5%',
                    '  - 实际: 第一层总佣金 - 下属所有层佣金',
                    '  - 如果下属没业绩，第一层拿不到佣金',
                    '',
                    '第二层 (OC619-01):',
                    '  - 计算: (自己业绩 + 下属第三层业绩) × 20%', 
                    '  - 实际: 第二层总佣金 - 下属第三层佣金',
                    '  - 如果下属没业绩，第二层拿不到佣金',
                    '',
                    '第三层 (OC619-01-01/OC619-01-02/等):',
                    '  - 计算: 全部第三层业绩总和 × 5%',
                    '  - 分配: 平分给所有有业绩的第三层代理',
                    '  - 只能拿自己这层的佣金',
                    '',
                    '其他代理 (BELLA/肥子代理 638/WS/等):',
                    '  - 计算: 自己总金额 × 30%'
                ]
            })
            rules_df.to_excel(writer, sheet_name='Commission_Rules', index=False)

        output.seek(0)
        
        # 纯英文文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"updated_commission_calculation_{timestamp}.xlsx"
        
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/")
def root():
    return {
        "message": "Excel Sorting and Updated Commission Calculation API",
        "endpoints": {
            "/upload-excel/": "Preview data with sorting and commission calculation",
            "/export-sorted/": "Fully sorted export with updated commission calculation"
        },
        "commission_rules": {
            "OC619第一层": "团队总业绩 × 5% - 下属所有佣金",
            "OC619第二层": "自己及下属业绩 × 20% - 下属第三层佣金", 
            "OC619第三层": "全部第三层业绩总和 × 5% (平分)",
            "其他代理": "自己总金额 × 30%"
        }
    }
