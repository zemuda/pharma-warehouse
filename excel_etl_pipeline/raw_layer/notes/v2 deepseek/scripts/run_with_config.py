# # run_with_config.py
# #!/usr/bin/env python3
# """
# Running the Excel to Parquet pipeline with YAML configuration
# """

# import sys
# from pathlib import Path

# # 添加当前目录到 Python 路径
# sys.path.append(str(Path(__file__).parent))

# from raw_layer.scripts.config_manager import config_manager
# import excel_parquet_pipeline


# def main():
#     """使用 YAML 配置运行管道"""
#     config = config_manager.get_all()

#     print("=== Excel to Parquet Pipeline ===")
#     print(f"配置文件: {config_manager.config_path}")
#     print(f"输入目录: {config['raw_layer']['input_dir']}")
#     print(f"输出目录: {config['raw_layer']['output_dir']}")
#     print(
#         f"数据库: {config['database']['user']}@{config['database']['host']}:{config['database']['port']}/{config['database']['name']}"
#     )
#     print("=" * 40)

#     # 运行管道
#     excel_parquet_pipeline.main()


# if __name__ == "__main__":
#     main()
