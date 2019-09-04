# -*- coding:UTF-8 -*-
import cx_Oracle
import datetime
import os

# 设置字符集与oracle一致，不然insert中文乱码
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
print('====beging...')
# 获取需要判断的数据信息
startTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
print('===========================',startTime,'============================')

DB_UserName = ""
DB_UserPwd = ""
DB_ConnectStr = ""


def Oracle_Query(SqlStr, debug=0):
    "Execute oracle query, and return data_list"
    conn = cx_Oracle.connect(DB_UserName, DB_UserPwd, DB_ConnectStr)
    data_list = []
    cursor = conn.cursor()
    try:
        cursor.execute(SqlStr)
        while 1:
            rs = cursor.fetchone()
            if rs == None:
                break
            data_list.append(rs)
        if debug:
            fieldnames = []
            for field in cursor.description:
                fieldnames.append(field[0])
            print(fieldnames)
            print(data_list)
            print("Query success!")
    except:
        print("Exec sql failed: %s" % SqlStr)
    finally:
        cursor.close()
        # conn.close()
        return data_list


def Oracle_Exec(SqlStr):
    "Execute oracle command"
    conn = cx_Oracle.connect(DB_UserName, DB_UserPwd, DB_ConnectStr)
    cursor = conn.cursor()
    try:
        cursor.execute(SqlStr)
        conn.commit()
        return True
    except:
        conn.rollback()
        print("Exec sql failed: %s" % SqlStr)
        return False
    finally:
        cursor.close()
    # conn.close()


# 判断坐标是否再坐标圈内
def is_pt_in_poly(aLon, aLat, pointList):
    '''''
    :param aLon: double 经度
    :param aLat: double 纬度
    :param pointList: list [(lon, lat)...] 多边形点的顺序需根据顺时针或逆时针，不能乱
    '''
    iSum = 0
    iCount = len(pointList)
    if (iCount < 3):
        return False
    # print("iCount = " + str(iCount))
    for i in range(iCount):
        pLon1 = pointList[i][0]
        pLat1 = pointList[i][1]
        if (i == iCount - 1):
            pLon2 = pointList[0][0]
            pLat2 = pointList[0][1]
        else:
            # print(i+1)
            try:
                pLon2 = pointList[i + 1][0]
                pLat2 = pointList[i + 1][1]
            except IndexError:
                break

        ###转换数值类型
        pLon1 = float(pLon1)
        pLat1 = float(pLat1)
        pLon2 = float(pLon2)
        pLat2 = float(pLat2)
        aLat = float(aLat)
        aLon = float(aLon)

        if ((aLat >= pLat1) and (aLat < pLat2)) or ((aLat >= pLat2) and (aLat < pLat1)):

            if (abs(pLat1 - pLat2) > 0):

                pLon = pLon1 - ((pLon1 - pLon2) * (pLat1 - aLat)) / (pLat1 - pLat2)

                if (pLon < aLon):
                    iSum += 1

    if (iSum % 2 != 0):
        return True
    else:
        return False


# 格式化从excel中得到的内容
# def get_file_row(row,file_name):
#     wb_1 = openpyxl.load_workbook('%s' % file_name)
#     ws_1 = wb_1.active
#     colC = ws_1['%s' % row]
#     list_1 = [x for x in colC]
#     list_2 = []
#     for i in list_1:
#         list_2.append(i.value)
#     return list_2

# 查询获得基站的名称、经度、纬度
def get_station_lo_la():
    sql_sec_name_lo_la = "SELECT A.B_STATION_NAME,A.LONGITUDE,A.LATITUDE FROM B_BASE_STATION_INFO A"
    list_station_lo_la = Oracle_Query(sql_sec_name_lo_la)
    return list_station_lo_la


# 查询获得网格的id、名称、网格坐标圈 指定市
def get_point_2(org_id, orgLevel):
    sql_sec_orgid_orgname_point = "select r.org_code,r.org_name,r.point " \
                                  "from (" \
                                  "select a.org_code,a.org_name,a.p_org_id,a.org_level,b.point " \
                                  "from s_orgnization a,p_map_draw b " \
                                  "where a.org_id=b.org_id " \
                                  "and b.state='00A' " \
                                  "and a.state='00A' " \
                                  "and a.org_id in ( " \
                                  "select org_id from s_orgnization start with org_id='%s' " \
                                  "connect by prior org_id=p_org_id)) r,s_orgnization o " \
                                  "where r.p_org_id=o.org_id " \
                                  "and r.org_level='%s'" % (str(org_id), str(orgLevel))
    list_orgid_orgname_point = Oracle_Query(sql_sec_orgid_orgname_point)
    return list_orgid_orgname_point


# 查询获得网格的id、名称、网格坐标圈
def get_point():
    sql_sec_orgid_orgname_point = "select a.org_id,a.org_name,b.point from s_orgnization a " \
                                  "left join p_map_draw b on b.org_id = a.org_id" \
                                  " where a.org_level='5'"
    list_orgid_orgname_point = Oracle_Query(sql_sec_orgid_orgname_point)
    return list_orgid_orgname_point


# 格式化cblob字段
def expand_list(tList):
    mList = tList.read().split(";")
    flag = 0
    for i in mList:
        mList[flag] = mList[flag].split(',')
        flag += 1
    return mList


# 修改基站grid_id   bak表
def update_station_grid_id(gridId, bStationName):
    sqlUpdateGridId = "update b_base_station_info_bak a " \
                      "set a.grid_id='%s' " \
                      "where a.b_station_name='%s'" % (str(gridId), bStationName)
    updateResult = Oracle_Exec(sqlUpdateGridId)
    return updateResult


# 对比两个b_station_name的grid_id是否相同
def judge_station_name(stationName):
    sqlGridIdFromInfo = " select grid_id from b_base_station_info_bak where b_station_name = '%s'" % stationName
    sqlGridIdFromMid = "select grid_id from B_STATION_GRID_MID where b_station_name = '%s'" % stationName
    gridIdFromInfo = Oracle_Query(sqlGridIdFromInfo)
    gridIdFromMid = Oracle_Query(sqlGridIdFromMid)
    if gridIdFromInfo == gridIdFromMid:
        return False
    else:
        return gridIdFromMid


# 格式化orgIdNamePoint
def expand_orgidnamepoint(orgIdNamePoint):
    flag = 0
    for i in orgIdNamePoint:
        if i[2] is not None:
            orgIdNamePoint[flag] = list(orgIdNamePoint[flag])
            orgIdNamePoint[flag][2] = expand_list(i[2])
        else:
            continue
        flag += 1
    return orgIdNamePoint


# 获取数据入中间表
def in_station_mid_table(stationLoLa, orgIdNamePoint):
    for station_name, station_lo, station_la in stationLoLa:  # 获取基站的经纬度
        for ord_id, org_name, org_point in orgIdNamePoint:  # 获取网格的相关内容 (id、name、point list)
            judge_result = is_pt_in_poly(station_lo, station_la, org_point)
            if judge_result:
                sql_insert_b_station_grid_mid = "insert into b_station_grid_mid (org_name,grid_id,b_station_name) " \
                                                "values ('%s','%s','%s')" % (org_name, ord_id, station_name)
                Oracle_Exec(sql_insert_b_station_grid_mid)
                break


# 对照中间表修改正式表中的所有数据
def updata_station_all():
    sqlSecStationNameFromMid = "select b_station_name from B_STATION_GRID_MID"
    stationNameList = Oracle_Query(sqlSecStationNameFromMid)
    for stationNameTup in stationNameList:
        gridId = judge_station_name(stationNameTup[0])
        if gridId:
            print(stationNameTup[0])
            print(gridId)
            update_station_grid_id(gridId[0][0], stationNameTup[0])
        else:
            continue


# 获取小区的名称以及经纬度
def get_cell_lo_la(cityId, dayID):
    """
    :param cityId: 小区所在的城市ID
    :return:
    """
    sqlGetCellIdLoLa = "SELECT A.CELL_ID,A.LONGITUDE,A.LATITUDE " \
                       "FROM B_SUBDISTRICT_INFO  A " \
                       "WHERE CITY_ID='%s' " \
                       "AND A.LONGITUDE IS NOT NULL " \
                       "AND A.LATITUDE IS NOT NULL " \
                       "AND A.DAY_ID='%s'" % (str(cityId), str(dayID))
    listCellLoLa = Oracle_Query(sqlGetCellIdLoLa)
    return listCellLoLa


# 判断小区的结果并录入中间表
def in_cell_mid_table(cellLoLa, orgIdNamePoint, orgLevel, dayId, cityId):
    """
    :param cellLoLa: 小区的坐标信息
    :param orgIdNamePoint: 网格的坐标范围信息
    :param orgLevel: 网格等级
    :return: 无返回值
    """
    for cellId, cellLo, cellLa in cellLoLa:  # 获取基站的经纬度
        for ord_id, org_name, org_point in orgIdNamePoint:  # 获取网格的相关内容 (id、name、point list)
            judge_result = is_pt_in_poly(cellLo, cellLa, org_point)
            if judge_result:
                sql_insert_b_cell_mid = "insert into b_cell_mid (day_id,org_name,grid_id,city_id,org_level,cell_id) " \
                                        "values ('%s','%s','%s','%s','%s','%s')" % (
                                        str(dayId), org_name, ord_id, str(cityId), str(orgLevel), cellId)
                Oracle_Exec(sql_insert_b_cell_mid)
                break


# 对比两个cell_id的region_id是否相同
def judge_cell_id_region(cellId):
    sqlGridIdFromInfo = " select region from B_SUBDISTRICT_INFO where cell_id = '%s'" % cellId
    sqlGridIdFromMid = "select grid_id from B_CELL_MID where cell_id = '%s'" % cellId
    gridIdFromInfo = Oracle_Query(sqlGridIdFromInfo)
    gridIdFromMid = Oracle_Query(sqlGridIdFromMid)
    if gridIdFromInfo == gridIdFromMid:
        return False
    else:
        return gridIdFromMid


# 修改基站grid_id   bak表
def update_cell_region(gridId, cellId):
    sqlUpdateGridId = "update B_SUBDISTRICT_INFO a " \
                      "set a.region='%s' " \
                      "where a.cell_id='%s'" % (str(gridId), cellId)
    updateResult = Oracle_Exec(sqlUpdateGridId)
    return updateResult


# 对照中间表修改正式表中的所有数据 update小区region_id
def updata_cell_region_all():
    sqlSecCellIdFromMid = "select cell_id from B_CELL_MID"
    cellIdList = Oracle_Query(sqlSecCellIdFromMid)
    for cellIdTup in cellIdList:
        gridId = judge_cell_id_region(cellIdTup[0])
        if gridId:
            print(cellIdTup[0])
            print(gridId)
            update_cell_region(gridId[0][0], cellIdTup[0])
        else:
            continue
