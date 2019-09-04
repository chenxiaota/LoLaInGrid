import getlola
import datetime
import threading


if __name__ == '__main__':
    threadl = []
    sqlTruncateMid = "truncate table b_station_grid_mid"
    getlola.Oracle_Exec(sqlTruncateMid)

    #获取基站的名称和经纬度
    station_lo_la = getlola.get_station_lo_la()
    #获取指定区域网格的id、name、point
    orgid_orgname_point = getlola.get_point_2('1000513','5')
    orgid_orgname_point = getlola.expand_orgidnamepoint(orgid_orgname_point)
    #print(orgid_orgname_point[0])
    # 对station进行分组

    flag = int(len(station_lo_la)/4)
    stationList1 = [ x for x in station_lo_la[:flag]]
    stationList2 = [ x for x in station_lo_la[flag:flag*2]]
    stationList3 = [ x for x in station_lo_la[flag*2:flag*3]]
    stationList4 = [ x for x in station_lo_la[flag*3:]]

    threadl.append(threading.Thread(target=getlola.in_station_mid_table, args=(stationList1,orgid_orgname_point,)))
    threadl.append(threading.Thread(target=getlola.in_station_mid_table, args=(stationList2, orgid_orgname_point,)))
    threadl.append(threading.Thread(target=getlola.in_station_mid_table, args=(stationList3, orgid_orgname_point,)))
    threadl.append(threading.Thread(target=getlola.in_station_mid_table, args=(stationList4, orgid_orgname_point,)))

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
