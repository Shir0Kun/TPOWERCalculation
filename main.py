from fastapi import FastAPI, UploadFile, File, Response
import io
import pandas as pd
from openpyxl import load_workbook
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
    numbers = re.findall(r'\d+', agent_str)
    return int(numbers[0]) if numbers else 0

def sort_by_agent_number(df):
    """按代理序号排序：先按上分地方，再按代理序号"""
    if '上分地方' in df.columns and '代理序号' in df.columns:
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
                agent_type = 1
            elif "OUT" in agent_upper:
                agent_type = 2
            else:
                agent_type = 3
            
            agent_number = extract_agent_number(agent)
            
            return (location_order.get(location, 99), agent_type, agent_number, location, agent)
        
        df_sorted = df.copy()
        df_sorted['sort_key'] = df_sorted.apply(sort_key, axis=1)
        df_sorted = df_sorted.sort_values('sort_key')
        df_sorted = df_sorted.drop('sort_key', axis=1)
        return df_sorted.reset_index(drop=True)
    else:
        return df

def calculate_hierarchical_commission(df):
    """计算层级佣金分配"""
    hierarchy = {
        "OC619": {"rate": 0.05, "level": 1, "children": ["OC619-01", "OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]},
        "OC619-01": {"rate": 0.20, "level": 2, "children": ["OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]},
        "OC619-01-01": {"rate": 0.05, "level": 3},
        "OC619-01-02": {"rate": 0.05, "level": 3},
        "OC619-01-03": {"rate": 0.05, "level": 3},
        "OC619-01-01-01": {"rate": 0.05, "level": 3},
        "肥子代理 638": {"rate": 0.30, "level": "其他"},
        "BELLA": {"rate": 0.30, "level": "其他"},
        "WS": {"rate": 0.30, "level": "其他"}
    }
    
    # 计算每个地点的总金额
    location_totals = {}
    for location in df['上分地方'].dropna().unique():
        if location:
            location_data = df[df['上分地方'] == location]
            location_totals[location] = location_data['金额'].sum()
    
    # 计算层级佣金
    commission_results = {}
    
    # 计算每个层级的业绩总和
    level_totals = {1: 0, 2: 0, 3: 0}
    
    # 确定每个地点属于哪个层级
    location_levels = {}
    for location in location_totals.keys():
        location_str = str(location)
        level = None
        
        if "OC619-01-01-01" in location_str:
            level = 3
        elif "OC619-01-01" in location_str or "OC619-01-02" in location_str or "OC619-01-03" in location_str:
            level = 3
        elif "OC619-01" in location_str:
            level = 2
        elif "OC619" in location_str:
            level = 1
        else:
            level = "其他"
        
        location_levels[location] = level
        if level in [1, 2, 3]:
            level_totals[level] += location_totals[location]
    
    # 计算各层总业绩（包含下层）
    level_cumulative_totals = {
        1: level_totals[1] + level_totals[2] + level_totals[3],
        2: level_totals[2] + level_totals[3],
        3: level_totals[3]
    }
    
    # 为每个地点计算佣金
    for location, total_amount in location_totals.items():
        location_str = str(location)
        level = location_levels[location]
        
        if level in [1, 2, 3]:
            commission_breakdown = {}
            
            if level == 1:
                commission_breakdown["OC619"] = level_cumulative_totals[1] * hierarchy["OC619"]["rate"]
            elif level == 2:
                commission_breakdown["OC619-01"] = level_cumulative_totals[2] * hierarchy["OC619-01"]["rate"]
            elif level == 3:
                third_level_agent = None
                for agent in ["OC619-01-01-01", "OC619-01-01", "OC619-01-02", "OC619-01-03"]:
                    if agent in location_str:
                        third_level_agent = agent
                        break
                
                if third_level_agent:
                    commission_breakdown[third_level_agent] = level_cumulative_totals[3] * hierarchy[third_level_agent]["rate"]
                else:
                    commission_breakdown["第三层代理"] = level_cumulative_totals[3] * 0.05
            
            commission_results[location] = {
                "总金额": total_amount,
                "层级": f"第{level}层",
                "佣金分配": commission_breakdown,
                "计算基础": {
                    "第一层业绩": level_totals[1],
                    "第二层业绩": level_totals[2],
                    "第三层业绩": level_totals[3],
                    "第一层计算基础": level_cumulative_totals[1],
                    "第二层计算基础": level_cumulative_totals[2],
                    "第三层计算基础": level_cumulative_totals[3]
                }
            }
        else:
            agent_key = None
            for key in ["肥子代理 638", "BELLA", "WS"]:
                if key in location_str:
                    agent_key = key
                    break
            
            if agent_key is None:
                agent_key = location_str
            
            commission_breakdown = {agent_key: total_amount * 0.30}
            
            commission_results[location] = {
                "总金额": total_amount,
                "层级": "其他代理",
                "佣金分配": commission_breakdown,
                "计算基础": {"自己业绩": total_amount}
            }
    
    return commission_results

@app.post("/export-sorted/")
async def export_sorted(file: UploadFile = File(...)):
    """完全排序导出 - 先按上分地方，再按代理序号，包含层级佣金计算"""
    try:
        contents = await file.read()
        df, error = process_excel_data(contents)
        
        if error:
            return {"error": error}

        # 完全排序：先按上分地方，再按代理序号
        df_sorted = sort_by_agent_number(df)
        
        # 计算层级佣金
        commission_results = calculate_hierarchical_commission(df)
        
        # 准备佣金数据
        commission_data = []
        total_commission_by_agent = {}
        
        for location, result in commission_results.items():
            row_data = {
                '上分地方': location,
                '总金额': result["总金额"],
                '层级': result["层级"]
            }
            
            # 添加计算基础信息
            if "计算基础" in result:
                for key, value in result["计算基础"].items():
                    row_data[f'计算基础_{key}'] = value
            
            # 添加佣金分配
            for agent, commission in result["佣金分配"].items():
                row_data[f'佣金_{agent}'] = commission
                
                if agent not in total_commission_by_agent:
                    total_commission_by_agent[agent] = 0
                total_commission_by_agent[agent] += commission
            
            commission_data.append(row_data)

        # 导出 - 只保留两个工作表
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet 1: 层级佣金计算
            commission_df = pd.DataFrame(commission_data)
            commission_df.to_excel(writer, sheet_name='Hierarchical_Commission', index=False)
            
            # Sheet 2: 佣金汇总
            summary_data = []
            for agent, total_commission in total_commission_by_agent.items():
                summary_data.append({
                    '代理层级': agent,
                    '总佣金': total_commission
                })
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Commission_Summary', index=False)

        output.seek(0)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"commission_report_{timestamp}.xlsx"
        
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        return {"error": str(e)}

@app.get("/")
def root():
    return {
        "message": "Excel Commission Calculation API",
        "endpoint": {
            "/export-sorted/": "Upload Excel file to get hierarchical commission calculation"
        },
        "features": {
            "commission": "Hierarchical commission calculation for OC619 series",
            "export": "2-sheet Excel report with commission details"
        }
    }

