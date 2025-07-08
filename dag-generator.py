import shutil
import re
import pandas

DAG_TEMPLATE = "C:/gitHub/dag-generator/tutorial_dag.py"
FILE_DESTINATION = "C:/gitHub/dag-generator/"
CSV_FILE = ''


def read_dag_template(template_dag_path):
    print('[START] read_dag_template')
    try:
        with open(template_dag_path, 'r', encoding='utf-8') as f:
            temp_dag_content = f.read()
    except FileNotFoundError:
        print(f"파일을 찾을 수 없음: {template_dag_path}")
        return None
    except Exception as e:
        print(f"오류 발생: {e}")
        return None
    return temp_dag_content


def parse_dag_variable(csv_row, temp_dag_content):
    print('[START] parse_dag_variable')

    # 필요한 곳 파싱하기
    modified_content = re.sub('"""@description"""', '"This is a Test"', temp_dag_content)
    modified_content = re.sub('"""@description"""', '"This is a Test"', temp_dag_content)
    modified_content = re.sub('"""@description"""', '"This is a Test"', temp_dag_content)
    modified_content = re.sub('"""@description"""', '"This is a Test"', temp_dag_content)
    return modified_content


def generate_dag(modified_content, copy_file_name):
    print('[START] generate_dag')    
    try:
        with open(copy_file_name, 'w', encoding='utf-8') as file:
            file.write(modified_content)
        print(f"{copy_file_name} 파일의 내용이 성공적으로 생성.")
    except Exception as e:
        print(f"오류 발생: {e}")
        print(f"{copy_file_name} 파일 생성중 오류 발생.")

def read_csv_file():
    temp_dag_content = read_dag_template(DAG_TEMPLATE)

    if not temp_dag_content:
        # DAG 별 테이블 정보가 정리된 CSV 파일
        try:
            df = pandas.read_csv(CSV_FILE)
        except FileNotFoundError:
            print(f"파일을 찾을 수 없음: {CSV_FILE}")
        except Exception as e:
            print(f"오류 발생: {e}")
        
        for row in df.itertuples(index=False):
            # test = row.name + row.test
            modified_content = parse_dag_variable(row, temp_dag_content)
            file_name = "test.py"
            copy_file_name = FILE_DESTINATION + file_name
            generate_dag(modified_content, copy_file_name)
        
if __name__ == "__main__":
    read_csv_file()
    # print('오예')

