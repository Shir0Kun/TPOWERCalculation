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

def calculate_hierarchical_commission(df):
    """计算层级佣金分配"""
    # 按层级分类
    hierarchy_levels = {
        "第一层": ["OC619"],           # 第一层
        "第二层": ["OC619-01"],        # 第二层  
        "第三层": ["OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]  # 第三层
    }
    
    # 计算每个层级的业绩
    level_performance = {}
    commission_results = {}
    
    # 计算第三层业绩（最底层）
    third_layer_performance = 0
    third_layer_locations = []
    for location in hierarchy_levels["第三层"]:
        location_data = df[df['上分地方'].str.contains(location, na=False)]
        if len(location_data) > 0:
            location_amount = location_data['金额'].sum()
            third_layer_performance += location_amount
            third_layer_locations.append({
                "location": location,
                "amount": location_amount
            })
    
    level_performance["第三层"] = third_layer_performance
    
    # 计算第二层业绩（第二层 + 第三层）
    second_layer_performance = 0
    for location in hierarchy_levels["第二层"]:
        location_data = df[df['上分地方'].str.contains(location, na=False)]
        if len(location_data) > 0:
            second_layer_performance += location_data['金额'].sum()
    # 第二层业绩包括第三层
    second_layer_performance += third_layer_performance
    level_performance["第二层"] = second_layer_performance
    
    # 计算第一层业绩（第一层 + 第二层 + 第三层）
    first_layer_performance = 0
    for location in hierarchy_levels["第一层"]:
        location_data = df[df['上分地方'].str.contains(location, na=False)]
        if len(location_data) > 0:
            first_layer_performance += location_data['金额'].sum()
    # 第一层业绩包括所有下层
    first_layer_performance += second_layer_performance
    level_performance["第一层"] = first_layer_performance
    
    # 计算佣金
    commission_results = {
        "第一层": {
            "代理": "OC619",
            "业绩总额": first_layer_performance,
            "佣金率": "5%",
            "佣金金额": first_layer_performance * 0.05,
            "说明": "第一层业绩（自己 + 第二层 + 第三层）"
        },
        "第二层": {
            "代理": "OC619-01", 
            "业绩总额": second_layer_performance,
            "佣金率": "20%",
            "佣金金额": second_layer_performance * 0.20,
            "说明": "第二层业绩（自己 + 第三层）"
        },
        "第三层": {
            "代理": "第三层代理",
            "业绩总额": third_layer_performance,
            "佣金率": "5%", 
            "佣金金额": third_layer_performance * 0.05,
            "说明": "第三层业绩（仅自己层级）"
        }
    }
    
    # 添加第三层详细 breakdown
    third_layer_details = []
    for loc_info in third_layer_locations:
        third_layer_details.append({
            "代理": loc_info["location"],
            "业绩": loc_info["amount"],
            "佣金率": "5%",
            "佣金": loc_info["amount"] * 0.05
        })
    
    commission_results["第三层详情"] = third_layer_details
    
    return commission_results

def calculate_commission(location, total_amount):
    """计算普通代理佣金分配"""
    if "肥子代理 638" in str(location):
        # 肥子代理 638: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            "肥子代理 638": commission
        }
        return commission, commission_rate, breakdown
    
    elif "BELLA" in str(location).upper():
        # BELLA: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            "BELLA": commission
        }
        return commission, commission_rate, breakdown
    
    elif "WS" in str(location).upper():
        # WS: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            "WS": commission
        }
        return commission, commission_rate, breakdown
    
    else:
        # 其他代理: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            str(location): commission
        }
        return commission, commission_rate, breakdown

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

        # 计算每个地点的金额统计和佣金
        location_amounts = {}
        commission_calculations = {}
        
        for location, transactions in location_details.items():
            amounts = [t["金额"] for t in transactions if t["金额"] is not None]
            in_transactions = [t for t in transactions if "IN" in str(t["代理序号"]).upper()]
            out_transactions = [t for t in transactions if "OUT" in str(t["代理序号"]).upper()]
            
            if amounts:
                total_amount = sum(amounts)
                
                # 计算佣金（非OC619系列使用普通计算）
                if "OC619" not in str(location):
                    total_commission, commission_rate, commission_breakdown = calculate_commission(location, total_amount)
                    
                    location_amounts[location] = {
                        "总交易笔数": len(transactions),
                        "IN笔数": len(in_transactions),
                        "OUT笔数": len(out_transactions),
                        "总金额": total_amount,
                        "平均金额": total_amount / len(amounts),
                        "最大金额": max(amounts),
                        "最小金额": min(amounts)
                    }
                    
                    commission_calculations[location] = {
                        "佣金率": f"{commission_rate * 100}%",
                        "总佣金": total_commission,
                        "佣金分配": commission_breakdown
                    }

        # 计算OC619层级佣金
        oc619_commission = calculate_hierarchical_commission(df)
        
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
                "commission_calculation": commission_calculations,
                "oc619_hierarchical_commission": oc619_commission,
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
    """完全排序导出 - 包含层级佣金计算"""
    try:
        contents = await file.read()
        df, error = process_excel_data(contents)
        
        if error:
            return {"error": error}

        # 完全排序：先按上分地方，再按代理序号
        df_sorted = sort_by_agent_number(df)
        
        # 计算普通代理佣金
        commission_data = []
        total_commission_summary = {}
        
        locations = df_sorted['上分地方'].dropna().unique()
        for location in locations:
            if location and "OC619" not in str(location):
                location_data = df_sorted[df_sorted['上分地方'] == location]
                total_amount = location_data['金额'].sum()
                
                # 计算佣金
                total_commission, commission_rate, commission_breakdown = calculate_commission(location, total_amount)
                
                commission_data.append({
                    '上分地方': location,
                    '总金额': total_amount,
                    '佣金率': f"{commission_rate * 100}%",
                    '总佣金': total_commission,
                    **commission_breakdown
                })
                
                # 汇总总佣金
                for agent, amount in commission_breakdown.items():
                    if agent not in total_commission_summary:
                        total_commission_summary[agent] = 0
                    total_commission_summary[agent] += amount
        
        # 计算OC619层级佣金
        oc619_commission = calculate_hierarchical_commission(df_sorted)
        
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
            
            # 重命名列
            location_summary.columns = ['交易笔数', '总金额', '平均金额', '最小金额', '最大金额']
            location_summary.to_excel(writer, sheet_name='Location_Statistics')
            
            # Sheet 3: 普通代理佣金计算
            if commission_data:
                commission_df = pd.DataFrame(commission_data)
                commission_df.to_excel(writer, sheet_name='Commission_Calculation', index=False)
            
            # Sheet 4: OC619层级佣金计算
            hierarchical_data = []
            for level, info in oc619_commission.items():
                if level != "第三层详情":
                    hierarchical_data.append({
                        '层级': level,
                        '代理': info['代理'],
                        '业绩总额': info['业绩总额'],
                        '佣金率': info['佣金率'],
                        '佣金金额': info['佣金金额'],
                        '说明': info['说明']
                    })
            
            hierarchical_df = pd.DataFrame(hierarchical_data)
            hierarchical_df.to_excel(writer, sheet_name='OC619_Hierarchical', index=False)
            
            # Sheet 5: 第三层详细佣金
            third_layer_details = []
            for detail in oc619_commission.get("第三层详情", []):
                third_layer_details.append(detail)
            
            if third_layer_details:
                third_layer_df = pd.DataFrame(third_layer_details)
                third_layer_df.to_excel(writer, sheet_name='Third_Layer_Details', index=False)
            
            # Sheet 6: 佣金汇总
            summary_data = []
            # 添加普通代理佣金
            for agent, amount in total_commission_summary.items():
                summary_data.append({'代理': agent, '佣金金额': amount, '类型': '普通代理'})
            
            # 添加OC619层级佣金
            for level, info in oc619_commission.items():
                if level != "第三层详情":
                    summary_data.append({
                        '代理': info['代理'], 
                        '佣金金额': info['佣金金额'],
                        '类型': f'OC619-{level}'
                    })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Commission_Summary', index=False)
            
            # Sheet 7: 佣金规则说明
            rules_df = pd.DataFrame({
                '佣金分配规则': [
                    '普通代理佣金规则:',
                    '肥子代理 638: 总金额 × 30%',
                    'BELLA: 总金额 × 30%', 
                    'WS: 总金额 × 30%',
                    '其他代理: 总金额 × 30%',
                    '',
                    'OC619层级佣金规则:',
                    '第一层 (OC619): (自己业绩 + 第二层业绩 + 第三层业绩) × 5%',
                    '第二层 (OC619-01): (自己业绩 + 第三层业绩) × 20%',
                    '第三层 (OC619-01-01/02/03等): (仅自己业绩) × 5%',
                    '',
                    '注意: 如果下层没有业绩，上层不会获得该层的佣金'
                ]
            })
            rules_df.to_excel(writer, sheet_name='Commission_Rules', index=False)

        output.seek(0)
        
        # 纯英文文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"fully_sorted_with_hierarchical_commission_{timestamp}.xlsx"
        
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
        "message": "Excel Sorting and Hierarchical Commission Calculation API",
        "endpoints": {
            "/upload-excel/": "Preview data with sorting and commission calculation",
            "/export-sorted/": "Fully sorted export with hierarchical commission calculation"
        },
        "sorting_logic": {
            "primary": "上分地方 (BELLA → 肥子代理 638 → OC619-01-01 → WS → 其他)",
            "secondary": "代理序号类型 (OC IN → OC OUT → 其他)",
            "tertiary": "代理序号数字 (从小到大)"
        },
        "commission_rules": {
            "普通代理": "30% (肥子代理 638, BELLA, WS, 其他)",
            "OC619层级代理": [
                "第一层 (OC619): 自己+第二层+第三层业绩 × 5%",
                "第二层 (OC619-01): 自己+第三层业绩 × 20%", 
                "第三层 (OC619-01-01等): 仅自己业绩 × 5%"
            ]
        }
    }
