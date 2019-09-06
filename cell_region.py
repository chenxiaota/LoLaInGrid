# -*- coding:UTF-8 -*-

import datetime

import getlola

import threading


def exec_cell(cityId, cityLevel, dayId):
    threadl = []

    # 获取基站的名称和经纬度

    cell_lo_la = getlola.get_cell_lo_la(cityId, dayId)

    # 获取指定区域网格的id、name、point

    orgid_orgname_point = getlola.get_point_2(cityId, cityLevel)

    orgid_orgname_point = getlola.expand_orgidnamepoint(orgid_orgname_point)

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

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList1, orgid_orgname_point, cityLevel, dayId, cityId,)))

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList2, orgid_orgname_point, cityLevel, dayId, cityId,)))

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList3, orgid_orgname_point, cityLevel, dayId, cityId,)))

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList4, orgid_orgname_point, cityLevel, dayId, cityId,)))

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList5, orgid_orgname_point, cityLevel, dayId, cityId,)))

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList6, orgid_orgname_point, cityLevel, dayId, cityId,)))

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList7, orgid_orgname_point, cityLevel, dayId, cityId,)))

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList8, orgid_orgname_point, cityLevel, dayId, cityId,)))

    else:

        threadl.append(threading.Thread(target=getlola.in_cell_mid_table,

                                        args=(cellList8, orgid_orgname_point, cityLevel, dayId, cityId,)))

    for i in threadl:
        i.start()

    for i in threadl:
        i.join()


if __name__ == '__main__':

    cityLevel = '3'

    sqlGetDayId = 'SELECT MAX(DAY_ID) FROM B_SUBDISTRICT_INFO'

    tmpDayId = getlola.Oracle_Query(sqlGetDayId)
    dayId = tmpDayId[0][0]

    threadl = []

    listForCityId = ['1000250', '1000510', '1000511', '1000512', '1000513', '1000514', '1000515', '1000516', '1000517',

                     '1000518', '1000519', '1000523', '1000527']

    for cityId in listForCityId:

        try:

            print(cityId, "执行开始：", cityLevel, datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

            exec_cell(cityId, cityLevel, dayId)

            print(cityId, "==========执行结束：", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

        except:

            print(cityId, "执行失败 fail")

            print(cityId, "==========执行结束：", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

            continue

    endTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    print("开始时间为：" + getlola.startTime)

    print("结束时间为：" + endTime)
