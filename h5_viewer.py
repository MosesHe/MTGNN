import pandas as pd
import os
import argparse

def get_h5_info(file_path):
    """获取h5文件的基本信息"""
    try:
        with pd.HDFStore(file_path, mode='r') as store:
            print(f"\n{'='*50}")
            print(f"文件大小: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
            print(f"包含的表: {store.keys()}")
            print(f"{'='*50}")
    except Exception as e:
        print(f"读取文件时出错: {str(e)}")

def get_table_nrows(store, table_key):
    """安全地获取表的行数"""
    try:
        # 方法1：直接获取行数
        nrows = len(store[table_key])
        return nrows
    except:
        try:
            # 方法2：通过storer获取行数
            nrows = store.get_storer(table_key).nrows
            return nrows
        except:
            try:
                # 方法3：读取数据后获取行数
                nrows = store.select(table_key).shape[0]
                return nrows
            except Exception as e:
                print(f"无法获取表 {table_key} 的行数: {str(e)}")
                return None

def view_table_info(file_path, table_key):
    """查看特定表的信息"""
    try:
        with pd.HDFStore(file_path, mode='r') as store:
            if table_key not in store.keys():
                print(f"错误：表 {table_key} 不存在")
                return
            
            # 获取表的基本信息
            nrows = get_table_nrows(store, table_key)
            if nrows is not None:
                print(f"\n表名: {table_key}")
                print(f"总行数: {nrows}")
                print("\n前5行数据预览:")
                print(store[table_key].head())
                
                # 显示数据类型信息
                print("\n数据类型信息:")
                print(store[table_key].dtypes)
            
    except Exception as e:
        print(f"查看表信息时出错: {str(e)}")

def view_table_slice(file_path, table_key, start, rows):
    """查看表的特定行数据"""
    try:
        with pd.HDFStore(file_path, mode='r') as store:
            if table_key not in store.keys():
                print(f"错误：表 {table_key} 不存在")
                return
            
            nrows = get_table_nrows(store, table_key)
            if nrows is None:
                print("无法获取表的行数信息")
                return
                
            if start >= nrows:
                print(f"错误：起始行 {start} 超出表的总行数 {nrows}")
                return
            
            end = min(start + rows, nrows)
            print(f"\n显示 {table_key} 的第 {start} 到 {end} 行：")
            try:
                # 方法1：使用select
                data = store.select(table_key, start=start, stop=end)
                print(data)
            except:
                try:
                    # 方法2：使用常规索引
                    data = store[table_key].iloc[start:end]
                    print(data)
                except Exception as e:
                    print(f"无法读取数据切片: {str(e)}")
            
    except Exception as e:
        print(f"查看数据切片时出错: {str(e)}")

def interactive_viewer():
    """交互式查看器"""
    while True:
        file_path = input("\n请输入h5文件路径（输入q退出）: ").strip()
        if file_path.lower() == 'q':
            break
            
        if not os.path.exists(file_path):
            print("文件不存在，请重试")
            continue
            
        get_h5_info(file_path)
        
        while True:
            print("\n请选择操作：")
            print("1. 查看特定表的信息")
            print("2. 查看特定表的数据切片")
            print("3. 选择其他文件")
            print("4. 退出程序")
            
            choice = input("请输入选项（1-4）: ").strip()
            
            if choice == '1':
                table_key = input("请输入表名（例如 /table1）: ").strip()
                view_table_info(file_path, table_key)
            
            elif choice == '2':
                table_key = input("请输入表名（例如 /table1）: ").strip()
                try:
                    start = int(input("请输入起始行（从0开始）: ").strip())
                    rows = int(input("请输入要查看的行数: ").strip())
                    view_table_slice(file_path, table_key, start, rows)
                except ValueError:
                    print("请输入有效的数字")
            
            elif choice == '3':
                break
                
            elif choice == '4':
                return
            
            else:
                print("无效的选项，请重试")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='HDF5文件查看器')
    parser.add_argument('--file', type=str, help='要查看的HDF5文件路径')
    args = parser.parse_args()
    
    if args.file:
        get_h5_info(args.file)
    
    interactive_viewer() 