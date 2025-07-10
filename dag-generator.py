import shutil
import re
import pandas

###########################################################################
################################상수 설정하기################################
###########################################################################

DAG_TEMPLATE = "C:/gitHub/dag-generator/tutorial_dag.py"
FILE_DESTINATION = "C:/gitHub/dag-generator/"
CSV_FILE = ''
BDP_SYSTEM = 'dat'
BDP_USAGE = 'dev'

DB_HOST = ''
DB_PORT = ''
DB_USER = ''
DB_PW = ''
DB_NM = ''
DB_CHARSET = ''

###########################################################################
###########################################################################
###########################################################################

def read_file(template_dag_path):
    print('[START] read_file')
    try:
        with open(template_dag_path, 'r', encoding='utf-8') as file:
            temp_dag_content = file.read()
    except FileNotFoundError:
        print(f"[ERROR] 파일을 찾을 수 없음: {template_dag_path}")
        return None
    except Exception as e:
        print(f"[ERROR] {template_dag_path} 읽는중 오류 발생: {e}")
        return None
    return temp_dag_content


def parse_dag_variable(csv_row, temp_dag_content):
    # print('[START] parse_dag_variable')
    modified_content = temp_dag_content

    # 작업유형 분류 : 적재/관리/로그/삭제 (load/mng/log/del)

    # 필요한 곳 파싱하기
    modified_content = re.sub(
        '"""@description"""',
        '"""This is a Test\n\ttest"""',
        temp_dag_content
        )
    # modified_content = re.sub('"""@description"""', '"This is a Test"', temp_dag_content)
    # modified_content = re.sub('"""@description"""', '"This is a Test"', temp_dag_content)
    # modified_content = re.sub('"""@description"""', '"This is a Test"', temp_dag_content)
    return modified_content, True


def generate_dag(modified_content, copy_file_name):
    # print('[START] generate_dag')    
    try:
        with open(copy_file_name, 'w', encoding='utf-8') as file:
            file.write(modified_content)
    except Exception as e:
        print(f"[ERROR] {copy_file_name} 파일 생성중 오류 발생: {e}")

def read_csv_file():
    print('[START] read_csv_file')
    temp_dag_content = read_file(DAG_TEMPLATE)

    if temp_dag_content is not None:
        # DAG 별 테이블 정보가 정리된 CSV 파일
        try:
            df = pandas.read_csv(CSV_FILE, encoding='utf-8')
        except FileNotFoundError:
            print(f"[ERROR] 파일을 찾을 수 없음 : {CSV_FILE}")
        except Exception as e:
            print(f"[ERROR] {CSV_FILE} 읽는 중 오류 발생 : {e}")
        
        for row in df.itertuples(index=False):
            modified_content, success_boolean = parse_dag_variable(row, temp_dag_content)
            if success_boolean:
                file_name = row.DagID
                copy_file_name = FILE_DESTINATION + file_name
                generate_dag(modified_content, copy_file_name)
            else:
                print(f"[ERROR] {row.DagID} DAG 파싱중 오류 발생")

def just_parse():
    temp_dag_content = read_file(DAG_TEMPLATE)
    row = 1
    if temp_dag_content is not None:
        print(111111)
        modified_content = parse_dag_variable(row, temp_dag_content)
        file_name = "test.py"
        copy_file_name = FILE_DESTINATION + file_name
        generate_dag(modified_content, copy_file_name)    

if __name__ == "__main__":
    # read_csv_file()
    # just_parse()
    test_pk = 'bas_dt, cust_no, prvd, acno'
    s_list = test_pk.split(',')
    merge_condtion = ''
    for i in range(len(s_list)):
        if i != 0:
            merge_condtion = merge_condtion + ' AND target.' + s_list[i].strip() + ' = source.' + s_list[i].strip()
        else:
            merge_condtion = 'target.' + s_list[i].strip() + ' = source.' + s_list[i].strip()
    print(merge_condtion)

