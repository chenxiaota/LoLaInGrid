# -*- coding:UTF-8 -*-
import datetime
import getlola
import threading

if __name__ == '__main__':
    threadl = []
    sqlTruncateMid = "truncate table B_CELL_MID"
    getlola.Oracle_Exec(sqlTruncateMid)

    #获取基站的名称和经纬度
    cell_lo_la = getlola.get_cell_lo_la('1000513')
    #获取指定区域网格的id、name、point
    orgid_orgname_point = getlola.get_point_2('1000513','3')
    orgid_orgname_point = getlola.expand_orgidnamepoint(orgid_orgname_point)
    #print(orgid_orgname_point[0])
    # 对station进行分组

    flag = int(len(cell_lo_la)/8)
    cellList1 = [ x for x in cell_lo_la[:flag]]
    cellList2 = [ x for x in cell_lo_la[flag:flag*2]]
    cellList3 = [ x for x in cell_lo_la[flag*2:flag*3]]
    cellList4 = [ x for x in cell_lo_la[flag*3:flag*4]]
    cellList5 = [x for x in cell_lo_la[flag * 4:flag * 5]]
    cellList6 = [x for x in cell_lo_la[flag * 5:flag * 6]]
    cellList7 = [x for x in cell_lo_la[flag * 6:flag * 7]]
    cellList8 = [x for x in cell_lo_la[flag * 7:]]

    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList1, orgid_orgname_point,'3',)))
    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList2, orgid_orgname_point,'3',)))
    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList3, orgid_orgname_point,'3',)))
    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList4, orgid_orgname_point,'3',)))
    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList5, orgid_orgname_point,'3',)))
    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList6, orgid_orgname_point,'3',)))
    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList7, orgid_orgname_point,'3',)))
    threadl.append(threading.Thread(target=getlola.in_cell_mid_table, args=(cellList8, orgid_orgname_point,'3',)))

    for i in threadl:
        i.start()
    for i in threadl:
        i.join()

    #将基站信息找到的网格录入中间表
    #getlola.in_station_mid_table(station_lo_la,orgid_orgname_point)
    #对比中间表修改正式表中基站的grid_id
    #test.updata_station_all()
    #getlola.conn.close()

    endTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print("开始时间为：" + getlola.startTime)
    print("结束时间为：" + endTime)