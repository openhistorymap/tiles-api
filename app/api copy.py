from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, and_, or_
from sqlalchemy.dialects import postgresql
from geoalchemy2 import Geometry
from geoalchemy2.functions import GenericFunction, ST_AsMVTGeom,  ST_TileEnvelope
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
import mercantile
from functools import partial
import pyproj
from shapely.ops import transform
from shapely.geometry import shape, MultiPolygon, MultiPoint, MultiLineString
from flask_gzip import Gzip
import os
import json
import numpy
from redis import Redis

from flask import Flask
from flask import request, make_response
from flask import jsonify

from flask_cors import CORS

import xxhash
import importlib

POSTGRES = os.environ.get('POSTGRES', '51.15.160.236:25432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'admin')
POSTGRES_PASS = os.environ.get('POSTGRES_PASS', 'tgZWW3Dgze94FN9O')
POSTGRES_DBNAME = os.environ.get('POSTGRES_DBNAME', 'ohm')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_DB = int(os.environ.get('REDIS_DB', '3'))

TILES_LAYERS=os.environ.get('TILES_LAYERS', 'boundary,boundary_label,culture,culture_label,waterway,water,building,industrial,landuse,transportation,place,religion')

EXPANDER = os.environ.get('EXPANDER', 'https://github.com/openhistorymap/mapstyles/blob/master/expander.json')

EPHEMERAL_STEP = float(1)/12/31
PERSIST_STEP = float(1)/12

def get_step(layer):
    if layer in TILES_LAYERS.split(','):
        return EPHEMERAL_STEP
    else:
        return EPHEMERAL_STEP

def get_month(x, year_step = EPHEMERAL_STEP):
    yr = [int(x)+y for y in [x*year_step for x in range(0,12*31)]]
    nyr = sorted(yr + [x])
    i = nyr.index(x)
    return yr[i-1]

class ST_AsMVT(GenericFunction):
    type = BYTEA

def float_to_date(f):
    y = int(f)
    m = int((f-y)*12)+1
    d = int(((f-y)*12-(m-1))*30)
    return '{}-{}-{}'.format(y,m,d)

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    # pool_pre_ping should help handle DB connection drops
    
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(
            POSTGRES_USER, POSTGRES_PASS, POSTGRES, POSTGRES_DBNAME
        ), pool_size=20, max_overflow=0, pool_pre_ping=True,
        echo=False)
    db = engine.connect()

    r = Redis(REDIS_HOST, REDIS_PORT, REDIS_DB)

    exp = {
        "boundary": {
            "name": {
                "Rome": {
                    "color": "#8e001c"
                },
                "Lotharingia": {
                    "color": "#ddb318"
                },
                "Kingdom of Italy": {
                    "color": "#6397d0"
                }
            }
        }
    }

    metadata = MetaData()
    ohm_items = Table('items', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('ohm_from', Float, index=True),
        Column('ohm_to', Float, index=True),
        Column('layer', String, index=True),
        Column('properties', JSONB),
        Column('geom', Geometry(geometry_type='GEOMETRY', srid=3857)),
        Column('author', String, default='ohm'),
    )
    ohm_rels = Table('relations', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('ohm_from', Float, index=True),
        Column('ohm_to', Float, index=True),
        Column('layer', String, index=True),
        Column('properties', JSONB),
        Column('geom', Geometry(geometry_type='GEOMETRY', srid=3857)),
        Column('author', String, default='ohm'),
    )

    ohm_rel_members = Table('relation_members', metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('relation', Integer, index=True),
        Column('item', Integer, index=True),
        Column('role', String, index=True),
        Column('author', String, default='ohm'),
    )


    ohm_items_members = Table('item_node', metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('item', Integer, index=True),
        Column('node_id', Integer, index=True),
    )

    ohm_items_members = Table('item_arc', metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('item', Integer, index=True),
        Column('arc_id', Integer, index=True),
    )


    ohm_arcs = Table('arc', metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('node_1', Integer),
        Column('node_2', Integer),
        Column('direction', Integer),
    )

    ohm_points = Table('node', metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('geom', Geometry(geometry_type='POINT', srid=3857)),
    )

    project = partial(
        pyproj.transform,
        pyproj.Proj(init='epsg:4326'), # source coordinate system
        pyproj.Proj(init='epsg:3857')) # destination coordinate system

    @app.route('/')
    def index():
        return ''

    @app.route('/setup')
    def setup():
        metadata.create_all(engine)
        return jsonify({'result':'OK'})

    def pimp(data):
        ret = {}
        if data['layer'] in exp.keys():
            for k in exp[data['layer']]:
                if k in data:
                    ret = exp[data['layer']][k].get(data[k], {})
        out = ret.copy()
        out.update(data)
        return out


    def map_single(jdata):
        ss = shape(jdata['geometry'])
        ss = transform(project, ss)
        pdata = jdata['properties']
        #print(pdata)
        _from = jdata['from'] if 'from' in jdata else pdata['ohm:from:date']
        _to = jdata['to'] if 'to' in jdata else pdata['ohm:to:date']
        pdata = pimp(pdata)
        cs = [ss]
        if isinstance(ss, (MultiPolygon, MultiLineString, MultiPoint,)):
            cs = [its.buffer(0) for its in ss if its]
        ret = []
        pdata['$area'] = ss.area
        pdata['$length'] = ss.length
        if 'Poly' in ss.geom_type:
            ret.append(dict(
                ohm_from = _from,
                ohm_to = _to,
                layer = pdata['layer']+"_label",
                properties = pdata,
                geom = 'SRID=3857;' + ss.representative_point().wkt
            ))
        for s in cs: 
            ret.append(dict(
                ohm_from = _from,
                ohm_to = _to,
                layer = pdata['layer'],
                properties = pdata,
                geom = 'SRID=3857;' + ss.wkt
            ))
        return ret


    @app.route('/items', methods=['POST'])
    def saveItem():
        data = request.data
        jdata = json.loads(data)
        #print(len(jdata))
        if not isinstance(jdata, list):
            jdata = [jdata]
        jdata = list(map(map_single, jdata))
        #print(len(jdata))

        flat_list = []
        for sublist in jdata:
            for item in sublist:
                flat_list.append(item)

        r.rpush('store', *[json.dumps(fi) for fi in flat_list])
        #map(lambda x: db.execute('ohm_storeitem(\'{layer}\', {ohm_from}, {ohm_to}, {properties}, {x})'.format**(x)), flat_list)
        #db.execute(ohm_items.insert(), flat_list)

        return jsonify({'result': 'OK', 'items_added': len(flat_list)})

    def out_rel_feat(r):
        rets = []

        f = r['itms']
        n = r['rel']
        pp = f[0].properties
        min_ = f[0].properties['ohm:from:date']
        max_ = f[-1].properties['ohm:to:date']
        pp['ohm:from:date'] = min_
        pp['ohm:from:date:year'] = int(min_)
        pp['ohm:to:date'] = max_
        pp['ohm:to:date:year'] = int(max_)
        pp['relation'] = n

        fp = []
        for fpo in f:
            fpop = fpo.properties
            fpop['name'] = float_to_date(fpop['ohm:from:date'])
            fpop['relation'] = n
            rets.append({
                "type": "Feature",
                "properties": fpop,
                "geometry": json.loads(fpo.gg)
            })
            fp.append(
                json.loads(fpo.gg).get('coordinates'), 
            )

        rets.append({
            "type": "Feature",
            "properties": pp,
            "geometry": {
                "type": "LineString",
                "coordinates": fp
            }
        })

        return rets
        
    @app.route('/relation', methods=['POST'])
    def newRelation():
        data = request.data
        jdata = json.loads(data)
        if not isinstance(jdata, list):
            jdata = [jdata]
        jdata = list(map(map_single, jdata))

        flat_list = []
        for sublist in jdata:
            for item in sublist:
                flat_list.append(item)

        x = db.execute(ohm_items.insert(), flat_list)
        print(x)
        return jsonify({'result': 'OK', 'items_added': len(flat_list)})
        

    @app.route('/bots', methods=['GET'])
    @app.route('/bots/<bot>', methods=['GET'])
    def runBot(bot = 'movement'):
        r.rpush('bot', bot)
        #m = importlib.import_module("bot.{}".format(bot))
        #m.run(db, )

        return jsonify({'result': 'OK'})

        
    @app.route('/status', methods=['GET'])
    def status():
        ret = {
            'bots': r.llen('bot'),
            'store': r.llen('store')
        }
        return jsonify({'result': 'OK', 'status': ret})
        
    
    CORS(app, resources={r"*": {"origins": "*"}})
    Gzip(app)
    return app

app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9039', debug=True, threaded=True)
