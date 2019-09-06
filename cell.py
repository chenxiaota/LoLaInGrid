# -*- coding:UTF-8 -*-
import datetime
import getlola
import threading


def exec_cell(cityId, cityLevel_5, cityLevel_4, dayId):
    threadl = []
    # 获取基站的名称和经纬度
    cell_lo_la = getlola.get_cell_lo_la(cityId, dayId)
    # 获取指定区域网格的id、name、point 等级5
    orgid_orgname_point_5 = getlola.get_point_2(cityId, cityLevel_5)
    orgid_orgname_point_5 = getlola.expand_orgidnamepoint(orgid_orgname_point_5)
    # 获取指定区域网格的id、name、point 等级4
    orgid_orgname_point_4 = getlola.get_point_2(cityId, cityLevel_4)
    orgid_orgname_point_4 = getlola.expand_orgidnamepoint(orgid_orgname_point_4)

    flag = int(len(cell_lo_la) / 8)
    cellList1 = [x for x in cell_lo_la[:flag]]
    cellList2 = [x for x in cell_lo_la[flag:flag * 2]]
    cellList3 = [x for x in cell_lo_la[flag * 2:flag * 3]]
    cellList4 = [x for x in cell_lo_la[flag * 3:flag * 4]]
    cellList5 = [x for x in cell_lo_la[flag * 4:flag * 5]]
    cellList6 = [x for x in cell_lo_la[flag * 5:flag * 6]]
    cellList7 = [x for x in cell_lo_la[flag * 6:flag * 7]]
    cellList8 = [x for x in cell_lo_la[flag * 7:]]

    if flag > 0:
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList1, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList2, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList3, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList4, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList5, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList6, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList7, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList8, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))
    else:
        threadl.append(threading.Thread(target=getlola.in_cell_mid_table_grid_id,
                                        args=(cellList8, orgid_orgname_point_5, orgid_orgname_point_4, cityLevel_5,
                                              cityLevel_4, dayId, cityId,)))

    for i in threadl:
        i.start()
    for i in threadl:
        i.join()


if __name__ == '__main__':

    cityLevel_5 = '5'
    cityLevel_4 = '4'
    dayId = '20190828'
    threadl = []
    # sqlTruncateMid = "truncate table B_CELL_MID"
    # getlola.Oracle_Exec(sqlTruncateMid)

    # print(orgid_orgname_point[0])
    # 对station进行分组

    # 将基站信息找到的网格录入中间表
    # getlola.in_station_mid_table(station_lo_la,orgid_orgname_point)
    # 对比中间表修改正式表中基站的grid_id
    # test.updata_station_all()
    # getlola.conn.close()

    listForCityId = ['1000250', '1000510', '1000511', '1000512', '1000513', '1000514', '1000515', '1000516', '1000517',
                     '1000518', '1000519', '1000523', '1000527']

    for cityId in listForCityId:
        try:
            print(cityId, "执行开始：", cityLevel_5, datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
            exec_cell(cityId, cityLevel_5, cityLevel_4, dayId)
            print(cityId, "==========执行结束：", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
        except:
            print(cityId, "执行失败 fail")
            print(cityId, "==========执行结束：", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
            continue

    endTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print("开始时间为：" + getlola.startTime)
    print("结束时间为：" + endTime)
