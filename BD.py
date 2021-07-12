import psycopg2 as pg
from math import radians, cos, sin, asin, sqrt
import json

connection = pg.connect(user="app",
                        password="qwerty",
                        host="localhost",
                        port="5432",
                        database="student")

cursor = connection.cursor()

cursor.execute("select exists(select * from information_schema.tables WHERE table_name = %s)", ('wycieczki',))
db_exist = (cursor.fetchone()[0])
if (not db_exist):
    cursor.execute(open("DB.sql", "r").read())
    connection.commit()
    print(json.dumps({"status": "OK"}))

def node(node, lat, lon, description):
    cursor.execute("SELECT ST_SetSRID( ST_Point( %(lat)s, %(lon)s), 4326)", {"lat": lat, "lon": lon})
    geog = cursor.fetchall()[0][0]
    fun = "INSERT INTO nodes (node, geog, description) VALUES ( %(node)s, %(geog)s, %(description)s)"
    cursor.execute(fun, {"node": node, "geog": geog, "description": description})
    connection.commit()
    return json.dumps({"status": "OK"})


def catalog(version, nodes):
    if (len(nodes) < 2):
        return json.dumps({"status": "ERROR"})
    else:
        cursor.execute("INSERT INTO WYCIECZKI (version, nodes_ref, type) VALUES ( %(version)s, %(nodes)s, 1)",
                       {"version": version, "nodes": nodes})
        connection.commit()
    return json.dumps({"status": "OK"})


def trip(cyclist, date, version):
    dist = round(calculate_distance(version))
    cursor.execute("SELECT EXISTS(SELECT %(cyclist)s FROM cyclists c WHERE c.cyclist = %(cyclist)s)", {"cyclist": cyclist})
    exist = cursor.fetchall()[0][0]
    if (not exist):
        cursor.execute("INSERT INTO cyclists (cyclist, no_trips, distance) VALUES (%(cyclist)s, 1, %(dist)s )",
                       {"cyclist": cyclist, "dist": dist})
        connection.commit()
    else:
        cursor.execute(
            "UPDATE cyclists SET no_trips = no_trips + 1, distance = distance + %(dist)s WHERE cyclists.cyclist = cyclist",
            {"cyclist": cyclist, "dist": dist})
        connection.commit()
    cursor.execute(
        "INSERT INTO reservations(c_name, s_date, wycieczka_version) VALUES (%(cyclist)s, %(date)s, %(version)s )",
        {"cyclist": cyclist, "date": date, "version": version})
    connection.commit()
    return json.dumps({"status": "OK"})


def haversine(lat1, lon1, lat2, lon2):  # returns distance in km
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # km
    return c * r

def calculate_distance(version):
    postgreSQL_select_Query = "SELECT nodes_ref from wycieczki WHERE version = %(version)s"
    cursor.execute(postgreSQL_select_Query, {"version": version})
    nodes = cursor.fetchall()
    if nodes != []:
        nodes = nodes[0][0]
    else:
        return
    nodes_geo = []
    for i in range(len(nodes)):
        postgreSQL_select_Query = "SELECT (ST_AsEWKT(geog::geometry)) from nodes WHERE node = %s"
        cursor.execute(postgreSQL_select_Query, (nodes[i],))
        coordinates = cursor.fetchall()[0][0]
        cursor.execute("SELECT ST_X(%s)", (coordinates,))
        x = cursor.fetchall()[0][0]
        cursor.execute("SELECT ST_Y(%s)", (coordinates,))
        y = cursor.fetchall()[0][0]
        nodes_geo.append((x, y))
    res = 0
    for i in range(len(nodes_geo) - 1):
        res += haversine(nodes_geo[i][0], nodes_geo[i][1], nodes_geo[i + 1][0], nodes_geo[i + 1][1])
    return round(res * 1000)


def closest_nodes(ilat, ilon):
    postgreSQL_select_Query = "SELECT (ST_AsEWKT(geog::geometry)), node from nodes"
    cursor.execute(postgreSQL_select_Query, )
    nodes = cursor.fetchall()
    n_distance = []
    for i in nodes:
        coordinates = i[0]
        cursor.execute("SELECT ST_Y(%s)", (coordinates,))
        lon = cursor.fetchall()[0][0]
        cursor.execute("SELECT ST_X(%s)", (coordinates,))
        lat = cursor.fetchall()[0][0]
        n_distance.append([i[1], lat, lon, haversine(ilat, ilon, lat, lon)])

    output = dict()
    output["status"] = "OK"
    data = []
    if len(n_distance) < 3:
        it = len(n_distance)
    else:
        it = 3
    for i in range(it):
        node = n_distance[i][0]
        olat = n_distance[i][1]
        olon = n_distance[i][2]
        distance = round(n_distance[i][3] * 1000)
        data.append({"node": node, "olat": olat, "olon": olon, "distance": distance})
    data = sorted(data, key=lambda a: (a["distance"], a["node"]))
    output["data"] = data
    return (json.dumps(output))


# spwawda gdzie nocuje rowerzyst <cyclist> w dniu <date>
def stay_in(cyclist, date):
    postgreSQL_select_Query = "SELECT s_date, wycieczka_version from reservations WHERE c_name = %(cyclist)s"
    cursor.execute(postgreSQL_select_Query, {"cyclist": cyclist, "date": date, })
    reservations = cursor.fetchall()
    for i in reservations:
        postgreSQL_select_Query = "SELECT array_length(nodes_ref,1), nodes_ref FROM wycieczki WHERE version = %(version)s"
        cursor.execute(postgreSQL_select_Query, {"version": i[1], })
        t = cursor.fetchone()
        days_len = t[0]
        nodes = t[1]
        postgreSQL_select_Query = "SELECT EXTRACT(day FROM (SELECT %(s_date)s + INTERVAL '%(len)s day') - %(date)s)"
        cursor.execute(postgreSQL_select_Query, {"date": date, "s_date": i[0], "len": days_len, })
        day = cursor.fetchone()[0]
        if (day >= days_len or day <= 1):
            continue
        else:
            return nodes[int(day - 1)]


def party(icyclist, date):
    postgreSQL_select_Query = "SELECT (ST_AsEWKT(geog::geometry)), node FROM nodes WHERE node = %(node)s"
    icyclist_stay = stay_in(icyclist, date)
    if (icyclist_stay == None):
        return None
    cursor.execute(postgreSQL_select_Query, {"node": icyclist_stay})
    icyclist_stay_geog = cursor.fetchone()
    cursor.execute("SELECT ST_X(%s)", (icyclist_stay_geog[0],))
    icyclist_stay_point = [cursor.fetchall()[0][0]]
    cursor.execute("SELECT ST_Y(%s)", (icyclist_stay_geog[0],))
    icyclist_stay_point.append(cursor.fetchall()[0][0])
    postgreSQL_select_Query = "SELECT (ST_AsEWKT(geog::geometry)), node FROM nodes"
    cursor.execute(postgreSQL_select_Query, )
    all_nodes = cursor.fetchall()
    res = []
    for i in all_nodes:
        cursor.execute("SELECT ST_Y(%s)", (i[0],))
        lon = cursor.fetchall()[0][0]
        cursor.execute("SELECT ST_X(%s)", (i[0],))
        lat = cursor.fetchall()[0][0]
        distance = haversine(icyclist_stay_point[0], icyclist_stay_point[1], lat, lon)
        if (distance < 20):
            temp = json.loads(guests(i[1], date))
            for j in temp["data"]:
                j["ocyclist"] = j["cyclist"]
                j.pop("cyclist", None)
                if (j["ocyclist"] != icyclist):
                    j.pop("no_trip", None)
                    j["node"] = i[1]
                    j["distance"] = distance
                    res.append(j)
    res = sorted(res, key=lambda a: (a["distance"], a["ocyclist"]))
    return json.dumps({"status": "OK", "data": res})


# sprawdza kto nocuje w nodie <node> w dnie <date>
def guests(node, date):
    postgreSQL_select_Query = "SELECT * from cyclists"
    cursor.execute(postgreSQL_select_Query, )
    cyclists = cursor.fetchall()
    guests = []
    for i in cyclists:
        if stay_in(i[0], date) == node:
            guests.append({"cyclist": i[0]})
    return json.dumps({"status": "OK", "data": guests})


def cyclists(limit):
    data = []
    postgreSQL_select_Query = "SELECT * from cyclists"
    cursor.execute(postgreSQL_select_Query, )
    cyclists = sorted(cursor.fetchall(), key=lambda a: (a[2], a[1]))
    if (limit > len(cyclists)):
        limit = len(cyclists)
    for i in range(limit):
        data.append({"cyclist": cyclists[i][0], "no_trips": cyclists[i][1], "distance": cyclists[i][2]})
    return json.dumps({"status": "OK", "data": data})


def execute(command):
    fun = eval(command["function"])
    data = command["body"]
    return fun(**data)


def get_input():
    while True:
        command = input()
        print(execute(json.loads(command)))


get_input()

