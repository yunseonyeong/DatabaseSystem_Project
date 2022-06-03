import os, sys
import json
import math

def select_record (table, key) :
  metadata_path = os.getcwd() + "\\" + table + "_meta.json"
  bin_path = os.getcwd() + "\\" + table
  with open(metadata_path, 'r') as f:
    metadata = json.load(f)
  
  null_byte = math.ceil(len(metadata["table_info"])/8)
  for bin_num in metadata["binary_file"] :
    slotted_page_structure = []
    with open(bin_path + str(bin_num) + ".bin", "rb") as file :
      slotted_page = file.readlines()
      for slotted in slotted_page :
        for slot in slotted :
          slotted_page_structure.append(slot)
      
    free_start = read_2byte(slotted_page_structure[4], slotted_page_structure[5])
    idx = 8
    
    while(idx < free_start) :
      data_start = read_2byte(slotted_page_structure[idx], slotted_page_structure[idx+1])
      record_len =read_2byte(slotted_page_structure[idx-2], slotted_page_structure[idx-1])
      # print(record_len)
      idx += 4
      pk = ''
      if metadata["table_info"][0]["var_len_record"] == 'false':# 고정길이면
        # print(slotted_page_structure[data_start + null_byte : data_start + null_byte + metadata["table_info"][0]["max_len"]])
        for pk_byte in slotted_page_structure[data_start + null_byte : data_start + null_byte + metadata["table_info"][0]["max_len"]]:
          pk += chr(pk_byte)
        if pk == key :
          # 찾으려는 데이터 찾았음
          record = slotted_page_structure[data_start : data_start + record_len]
          # null -- offset -- data 
          # ["id" : "dd", "dd" ... ]
          null_bitmap = ''
          data_idx = null_byte
          value = [0]*len(metadata["table_info"])
          for i in range(null_byte):

            a = str(format(record[i], "b"))
            null_bitmap += '0' * (8-len(a)) + a

          for i in range(len(metadata["table_info"])):
            
            if null_bitmap[i] == '1' :
              # null 이면
              value[i] = "null"
            else : 
              if metadata["table_info"][i]["var_len_record"] == 'true' :
                # 가변길이면
                record_location = record[data_idx]
                record_size = record[data_idx+1]
                v = ''
                for rec in record[record_location:record_location + record_size]:
                  v += chr(rec)
                value[i] = v
                data_idx += 2

              else :
                #고정길이면
                rec_len = metadata["table_info"][i]["max_len"]
                v = ''
                for rec in record[data_idx : data_idx + rec_len]:
                  v += chr(rec)
                value[i] = v
                data_idx += rec_len

          return value


def select_column (table, key, col) :
  metadata_path = os.getcwd() + "\\" + table + "_meta.json"
  bin_path = os.getcwd() + "\\" + table
  with open(metadata_path, 'r') as f:
    metadata = json.load(f)
  
  null_byte = math.ceil(len(metadata["table_info"])/8)
  for bin_num in metadata["binary_file"] :
    slotted_page_structure = []
    with open(bin_path + str(bin_num) + ".bin", "rb") as file :
      slotted_page = file.readlines()
      for slotted in slotted_page :
        for slot in slotted :
          slotted_page_structure.append(slot)
      
    free_start = read_2byte(slotted_page_structure[4], slotted_page_structure[5])
    idx = 8
    
    while(idx < free_start) :
      data_start = read_2byte(slotted_page_structure[idx], slotted_page_structure[idx+1])
      record_len =read_2byte(slotted_page_structure[idx-2], slotted_page_structure[idx-1])
      # print(record_len)
      idx += 4
      pk = ''
      if metadata["table_info"][0]["var_len_record"] == 'false':# 고정길이면
        # print(slotted_page_structure[data_start + null_byte : data_start + null_byte + metadata["table_info"][0]["max_len"]])
        for pk_byte in slotted_page_structure[data_start + null_byte : data_start + null_byte + metadata["table_info"][0]["max_len"]]:
          pk += chr(pk_byte)
        if pk == key :
          # 찾으려는 데이터 찾았음
          record = slotted_page_structure[data_start : data_start + record_len]
          # null -- offset -- data 
          # ["id" : "dd", "dd" ... ]
          null_bitmap = ''
          data_idx = null_byte
          
          for i in range(null_byte):

            a = str(format(record[i], "b"))
            null_bitmap += '0' * (8-len(a)) + a

          for i in range(len(metadata["table_info"])):
            
            if null_bitmap[i] == '1' :
              # null 이면
              if metadata["table_info"][i]["column_name"] == col:
               return "null"
            else : 
              if metadata["table_info"][i]["var_len_record"] == 'true' :
                # 가변길이면
                record_location = record[data_idx]
                record_size = record[data_idx+1]
                v = ''
                for rec in record[record_location:record_location + record_size]:
                  v += chr(rec)
                if metadata["table_info"][i]["column_name"] == col:
                  return v
                data_idx += 2

              else :
                #고정길이면
                rec_len = metadata["table_info"][i]["max_len"]
                v = ''
                for rec in record[data_idx : data_idx + rec_len]:
                  v += chr(rec)

                data_idx += rec_len
                if metadata["table_info"][i]["column_name"] == col:
                  return v


# table 마다 column수가 다르니까 info수도 다름
def make_table(table, *infos):

  # big meta 만들기
  bigmeta = {}
  bigmeta_path = os.getcwd() + "\\bigmeta.json"
  bigmeta['num_table'] = 0
  bigmeta['page_size'] = 4096
  bigmeta['num_table'] += 1
  bigmeta['metafile_table'] = []
  bigmeta['metafile_table'].append(table + "_meta.json")

  with open(bigmeta_path, 'w') as outfile:
    json.dump(bigmeta, outfile)

  # meta data 만들기
  metadata_path = os.getcwd() + "\\" + table + "_meta.json"
  metadata = {}
  metadata["table_name"] = table
  metadata["table_info"] = []
  for info in infos : 
    metadata["table_info"].append({"column_name" : info[0], "max_len" : info[1], "var_len_record" : info[2]})

  metadata["binary_file"] = []

  with open(metadata_path, 'w') as f :
    json.dump(metadata, f)

# make table test

def var_len_record(table, info) :
  metadata_path = os.getcwd() + "\\" + table + "_meta.json"

  with open(metadata_path, 'r') as f :
    metadata = json.load(f)
  
  num_column = len(metadata["table_info"])

  # ex. 이건 차지하는 공간임. 실제론 컬럼 수가 8의 배수개는 아니겠지
  null_bit = math.ceil(num_column/8) * 8 * [0]
  index = math.ceil(num_column/8)
  real_data_len = 0

  # null 값 들어오면 null_bit에 반영

  for i, table_info in enumerate(metadata["table_info"]):
    if info[i] == "null" :
      null_bit[i] = 1
    # 레코드의 전체 바이트 수 계산해보자
    elif table_info["var_len_record"] == 'false':
      index += table_info["max_len"]
    elif table_info["var_len_record"] == 'true':
      # 가변길이면 offset, len 1byte씩 컬럼당 2byte씩
      index += 2
      # 진짜 데이터 길이
      real_data_len += len(info[i])
  
  var_record_format = [0] * (index + real_data_len)

  null_int = []
  offset_start_idx = math.ceil(num_column / 8)
  data_start_idx = index

  for i in range(math.ceil(num_column / 8)):
    t = int("".join(map(str,null_bit[i*8 : i*8+8])),2)
    null_int.append(t)

  for i,n in enumerate(null_int) :
    var_record_format[i] = n

  for i, value in enumerate(info):
    # 가변길이 필드면
    if value == "null" :
      continue
    if metadata["table_info"][i]["var_len_record"] == "true":
      var_record_format[offset_start_idx] = data_start_idx
      var_record_format[offset_start_idx+1] = len(value)
      for i,v in enumerate(value) :
        var_record_format[data_start_idx+i] = ord(v)

      # index 2가지 다 업데이트 해주기
      data_start_idx += len(value)
      offset_start_idx += 2
    
    else :
      # 고정길이 필드면
      for i,v in enumerate(value):
        var_record_format[offset_start_idx+i] = ord(v)

      # index 업데이트
      offset_start_idx += len(value) 
  return var_record_format

# slotted page 구조 만들자
def insert_record(table, var_record_format) :
  metadata_path = os.getcwd() + "\\" + table + "_meta.json"
  bigmeta_path = os.getcwd() + "\\" + "bigmeta.json"

  with open(metadata_path, 'r') as f :
    metadata = json.load(f)
  
  with open(bigmeta_path, 'r') as f :
    bigmeta = json.load(f)

  slotted_page_file_list = metadata["binary_file"]
  
  if len(slotted_page_file_list) == 0 :
    slotted_page_struct = [0] * bigmeta["page_size"]
    initial_free = bigmeta["page_size"]

    metadata["binary_file"].append(0)

    # data 채우기
    # num of entries & num of records (각 1byte)
    slotted_page_struct[0] += 1
    slotted_page_struct[1] += 1
    free_start = 10
    data_start = initial_free - len(var_record_format)
    freesize = data_start-10

    # freespace size (2byte)
    slotted_page_struct[2],slotted_page_struct[3] = make_2byte(freesize)
    # freespace location (2byte)
    slotted_page_struct[4], slotted_page_struct[5] = make_2byte(10)
    # record size (2byte)
    slotted_page_struct[6], slotted_page_struct[7] = make_2byte(len(var_record_format))
    # record location (2byte)
    slotted_page_struct[8], slotted_page_struct[9] = make_2byte(data_start)
 

    for i, var_record in enumerate(var_record_format) :
      slotted_page_struct[data_start+i] = var_record
    
    path = os.getcwd() + "\\" + table + "0" + ".bin"

    with open(path, 'wb') as f :
      f.write(bytes(slotted_page_struct))

    with open(metadata_path, "w") as f:
      json.dump(metadata, f)

  else :
    for slotted_page_file in slotted_page_file_list :
      slotted_page = []
      with open((os.getcwd() + "\\" + table + str(slotted_page_file) + ".bin"), "rb") as f :
        slotted_page_bin = f.readlines()
        for i in slotted_page_bin:
          for j in i:
            slotted_page.append(j)
      
      slotted_page_struct = slotted_page

      # free size 여유공간 계산
       
      freesize = read_2byte(slotted_page_struct[2], slotted_page_struct[3])

      # free_size : 공간 크기
      if freesize >= len(var_record_format) + 4 :
        # 삽입 가능
        slotted_page_struct[0] += 1
        slotted_page_struct[1] += 1
        freesize -= (len(var_record_format) + 4)
        # free size 크기 기록
        slotted_page_struct[2], slotted_page_struct[3] = make_2byte(freesize)

        # free space start 지점 읽기
        free_start = read_2byte(slotted_page_struct[4], slotted_page_struct[5])
        # offset, length 때문에 4바이트 밀림
        free_start += 4
        # free start update
        slotted_page_struct[4], slotted_page_struct[5] = make_2byte(free_start)

        # 1.size, 2.location 기록 >> free space 시작점부터
        slotted_page_struct[free_start-4], slotted_page_struct[free_start+1-4] =  make_2byte(len(var_record_format))
        # location 기록
        data_start = free_start+freesize
        slotted_page_struct[free_start+2-4], slotted_page_struct[free_start+3-4] = make_2byte(data_start)

        for i, var_record in enumerate(var_record_format) :
          slotted_page_struct[data_start+i] = var_record
          
        with open((os.getcwd() + "\\" + table + str(slotted_page_file) + ".bin"), "wb") as f :
          f.write(bytes(slotted_page_struct))

        break
  
    else:
      bin_num = slotted_page_file_list[-1] + 1
      bin_name = table + str(bin_num)
      bin_path = os.getcwd() + "\\" + bin_name + ".bin"

      slotted_page_struct = [0] * bigmeta["page_size"]
      initial_free = bigmeta["page_size"]

      metadata["binary_file"].append(bin_num)

      # data 채우기
      # num of entries & num of records (각 1byte)
      slotted_page_struct[0] += 1
      slotted_page_struct[1] += 1
      free_start = 10
      data_start = initial_free - len(var_record_format)
      freesize = data_start-10

      # freespace size (2byte)
      slotted_page_struct[2],slotted_page_struct[3] = make_2byte(freesize)
      # freespace location (2byte)
      slotted_page_struct[4], slotted_page_struct[5] = make_2byte(10)
      # record size (2byte)
      slotted_page_struct[6], slotted_page_struct[7] = make_2byte(len(var_record_format))
      # record location (2byte)
      slotted_page_struct[8], slotted_page_struct[9] = make_2byte(data_start)
  

      for i, var_record in enumerate(var_record_format) :
        slotted_page_struct[data_start+i] = var_record
      
      with open(bin_path, 'wb') as f :
        f.write(bytes(slotted_page_struct))

      with open(metadata_path, "w") as f:
        json.dump(metadata, f)




def make_2byte (a) :
  # a는 정수
  a = format(a, "b")
  a = "0" * (16-len(a)) + a
  a_one = int(a[0:8], 2)
  a_two = int(a[8:],2)

  return a_one, a_two

def read_2byte (a_one, a_two):
  a = a_one *256 + a_two
  return a



############################################################################################
# 실행 방법
# make_table("student", ("id", 2, "false"),("name", 10, "true"),("dept_name", 20, "true"))

# r1 = var_len_record("student", ["01", "kim", "comp"])
# insert_record("student", r1)

# r2 = var_len_record("student", ["02", "yun", "math"])
# insert_record("student", r2)

# r3 = var_len_record("student", ["03", "kim", "math"])
# insert_record("student", r3)

# r4 = var_len_record("student", ["04", "kang", "comp"])
# insert_record("student", r4)

# r5 = var_len_record("student", ["05", "park", "music"])
# insert_record("student", r5)

# r6 = var_len_record("student", ["06", "bae", "art"])
# insert_record("student", r6)

# page = []
# with open(os.getcwd() + "\\student0.bin", 'rb') as f :
#     result = f.readlines()
#     for re in result :
#       for r in re :
#         page.append(r)

# print(page)

  
# result1 = select_record("student", "02")
# print(result1)

# result2 = select_column("student", "02", "name")
# print(result2)
