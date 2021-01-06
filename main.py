import pymysql
from app import app
from db_config import mysql
from flask import jsonify
from flask import flash, request
from flask import Response
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sqlalchemy import create_engine


def getKelurahan():
    try:
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT * FROM tbl_kelurahan")
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


def getCountKelurahanByBulan(kelurahan, idPenyakit, bulan, tahun):
    try:
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT COUNT(*) AS Jumlah FROM tbl_data_penderita WHERE id_kelurahan = " + kelurahan 
                        + " AND id_penyakit = " + idPenyakit 
                        + " AND MONTH(tgl_kejadian) = " + bulan 
                        + " AND YEAR(tgl_kejadian) = " + tahun)
        rows = cursor.fetchall()
        count = 0
        for row in rows:
            count = row['Jumlah']
        return count
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

@app.route('/penderitaBaru')
def penderitaBaru():
    idPenyakit = request.args.get('id-penyakit',default = 1, type = int)
    tahun = request.args.get('tahun')
    try:
        kelurahan = getKelurahan()
        df = pd.DataFrame()
        for row in kelurahan:
            df = df.append(pd.DataFrame({"id_kelurahan":[row['id']], 
                        "kelurahan":row['nama_kelurahan'],
                        "longitude":row['longitude'],
                        "latitude":row['latitude'],
                        "januari":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '1',str(tahun)),
                        "februari":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '2', str(tahun)),
                        "maret":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '3', str(tahun)),
                        "april":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '4', str(tahun)),
                        "mei":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '5', str(tahun)),
                        "juni":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '6', str(tahun)),
                        "juli":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '7', str(tahun)),
                        "agustus":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '8', str(tahun)),
                        "september":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '9', str(tahun)),
                        "oktober":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '10', str(tahun)),
                        "november":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '11', str(tahun)),
                        "desember":getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '12', str(tahun)),
                        "total": getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '1', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '2', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '3', str(tahun))
                                 + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '4', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '5', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '6', str(tahun))
                                 + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '7', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '8', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '9', str(tahun))
                                 + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '10', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '11', str(tahun)) + getCountKelurahanByBulan(str(row['id']), str(idPenyakit), '12', str(tahun))
                        }))
        # df["total"] = df.sum(axis=1)
        df2 = df.drop(['id_kelurahan', 'kelurahan', 'longitude', 'latitude'], axis=1)
        minValue = df2['total'].min()
        maxValue = df2['total'].max()
        avgValue = df2['total'].mean()
        newAvg = round(avgValue,0)
        centroidMin = df2.loc[df2['total'] == minValue]
        centroidMin = centroidMin.iloc[0]
        centroidMin = centroidMin.values
        centroidMin[:-1]

        centroidAvg = df2.loc[df2['total'] == newAvg]
        centroidAvg = centroidAvg.iloc[0]
        centroidAvg = centroidAvg.values
        centroidAvg[:-1]

        centroidMax = df2.loc[df2['total'] == maxValue]
        centroidMax = centroidMax.iloc[0]
        centroidMax = centroidMax.values
        centroidMax[:-1]

        centroidAwal = np.array([centroidMin[:-1], centroidAvg[:-1], centroidMax[:-1]],np.int64)

        df2 = df2.drop(['total'], axis=1)
        kmeans = KMeans(n_clusters=3, init=centroidAwal)
        kmeans.fit(df2)

        df['cluster'] = kmeans.labels_
        df.reset_index(drop=True, inplace=True)
        return Response(df.to_json(orient="index"), mimetype='application/json')
        # return Response(df.to_csv(index=False), mimetype='text/csv')
    except Exception as e:
        print(e)
@app.route('/')
def index():
	try:
		conn = mysql.connect()
		cursor = conn.cursor(pymysql.cursors.DictCursor)
		cursor.execute("SELECT * FROM tbl_data_penderita")
		
		rows = cursor.fetchall()
		resp = jsonify(rows)
		resp.status_code = 200
		return resp
	except Exception as e:
		print(e)
	finally:
		cursor.close() 
		conn.close()

@app.route('/penderita')
def penderita():
    tahun = request.args.get('tahun')
    # jenisPenyakit = request.args.get('jenis-penyakit')
    db_connection_str = 'mysql+pymysql://bfc59d60bc4718:a58fcc55@us-cdbr-east-02.cleardb.com/heroku_f4425b09c2cb37b'
    db_connection = create_engine(db_connection_str)
    try:

        df = pd.read_sql("SELECT tbl_kelurahan.nama_kelurahan, \
                            tbl_kelurahan.longitude AS longitude, \
                            tbl_kelurahan.latitude AS latitude, \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian)= 1 then 1 ELSE 0 END ) AS 'jan', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian)= 2 then 1 ELSE 0 END ) AS 'feb', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian)= 3 then 1 ELSE 0 END ) AS 'mar', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian)= 4 then 1 ELSE 0 END ) AS 'apr', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 5 THEN 1 ELSE 0 END) AS 'mei', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 6 THEN 1 ELSE 0 END) AS 'jun', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 7 THEN 1 ELSE 0 END) AS 'jul', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 8 THEN 1 ELSE 0 END) AS 'agt', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 9 THEN 1 ELSE 0 END) AS 'sept', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 10 THEN 1 ELSE 0 END) AS 'okt', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 11 THEN 1 ELSE 0 END) AS 'nov', \
                            SUM(CASE WHEN MONTH(tbl_data_penderita.tgl_kejadian) = 12 THEN 1 ELSE 0 END) AS 'des', \
                            COUNT(tbl_data_penderita.id_penderita) AS 'total' \
                            FROM tbl_data_penderita LEFT JOIN tbl_kelurahan \
                            ON tbl_kelurahan.id=tbl_data_penderita.id_kelurahan \
                            WHERE YEAR(tbl_data_penderita.tgl_kejadian)= " + tahun + "\
                            GROUP BY tbl_data_penderita.id_kelurahan", con=db_connection)
        db_connection.dispose()
        df2 = df.drop(['nama_kelurahan', 'longitude', 'latitude'], axis=1)
        minValue = df2['total'].min()
        maxValue = df2['total'].max()
        avgValue = df2['total'].mean()
        newAvg = round(avgValue,0)
        centroidMin = df2.loc[df2['total'] == minValue]
        centroidMin = centroidMin.iloc[0]
        centroidMin = centroidMin.values
        centroidMin[:-1]

        centroidAvg = df2.loc[df2['total'] == newAvg]
        centroidAvg = centroidAvg.iloc[0]
        centroidAvg = centroidAvg.values
        centroidAvg[:-1]

        centroidMax = df2.loc[df2['total'] == maxValue]
        centroidMax = centroidMax.iloc[0]
        centroidMax = centroidMax.values
        centroidMax[:-1]

        centroidAwal = np.array([centroidMin[:-1], centroidAvg[:-1], centroidMax[:-1]],np.int64)

        df2 = df2.drop(['total'], axis=1)
        kmeans = KMeans(n_clusters=3, init=centroidAwal)
        kmeans.fit(df2)

        df['cluster'] = kmeans.labels_
    
        return Response(df.to_json(orient="index"), mimetype='application/json')
    except Exception as e:
        print(e)
        message = {
            'status': 400,
            'message': 'Bad Request',
        }
        resp = jsonify(message)
        resp.status_code = 400
        return resp
        

@app.errorhandler(404)
def not_found(error=None):
    message = {
        'status': 404,
        'message': 'Not Found: ' + request.url,
    }
    resp = jsonify(message)
    resp.status_code = 404
    return resp
		
if __name__ == "__main__":
    app.run()