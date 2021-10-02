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
import os
import json

from redis import Redis

from flask_gzip import Gzip
from flask import Flask, Response, stream_with_context
from flask import request, make_response
from flask import jsonify
from flask_cors import CORS

import xxhash

POSTGRES = os.environ.get('POSTGRES', '51.15.160.236:25432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'admin')
POSTGRES_PASS = os.environ.get('POSTGRES_PASS', 'tgZWW3Dgze94FN9O')
POSTGRES_DBNAME = os.environ.get('POSTGRES_DBNAME', 'ohm_prod')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_HOST_2 = os.environ.get('REDIS_HOST_2', REDIS_HOST)
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_PORT_2 = int(os.environ.get('REDIS_PORT_2', '6379'))
REDIS_DB = int(os.environ.get('REDIS_DB', '3'))
REDIS_DB_2 = int(os.environ.get('REDIS_DB_2', str(REDIS_DB+1)))
EXPIRY = int(os.environ.get('EXPIRY', str(6*60*60)))

TILES_LAYERS=os.environ.get('TILES_LAYERS', 'boundary_label,culture_label,waterway,building,industrial,transportation,place,boundary,culture,water,landuse')

EXPANDER = os.environ.get('EXPANDER', 'https://github.com/openhistorymap/mapstyles/blob/master/expander.json')

cached_layers = os.environ.get('CACHED_LAYERS', 'boundary,boundary_label,culture,culture_label').split(',')

limit_zoom = 4
bound_filter = {
    'field': 'admin_level',
    'value': lambda x: max(2,int(int(x)/2))
}
filters = {
    'boundary': bound_filter,
    'boundary_label': bound_filter,
    'culture': bound_filter,
    'culture_label': bound_filter,
}

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
    cache = Redis(REDIS_HOST, REDIS_PORT, REDIS_DB)


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
        Column('geom', Geometry()),
        Column('author', String, default='ohm'),
    )

    ohm_rel_members = Table('relation_members', metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('relation', Integer, index=True),
        Column('item', Integer, index=True),
        Column('role', String, index=True),
        Column('author', String, default='ohm'),
    )

    project = partial(
        pyproj.transform,
        pyproj.Proj(init='epsg:4326'), # source coordinate system
        pyproj.Proj(init='epsg:3857')) # destination coordinate system

    @app.route('/')
    def index():
        return ''

    @app.route('/<timeline>/<year>/<z>/<y>/<x>/vector.pbf')
    def properties(year,z,y,x, timeline = 'default'):
        def gen():
            yr = get_month(float(year))
            iz = int(z)
            iy = int(y)
            ix = int(x)
            layers = request.args.get('layers', TILES_LAYERS).split(',')
            debug = request.args.get('debug')
            zl = [2,4,6,8,12,20,24]
            zf = ([2] + list(filter(lambda x: x <= iz, zl)))[-1]
            qs = []
            for l in layers:
                params = {'zf': zf, 'layer': l, 'year': yr, 'yeart': yr + get_step(l),  'z': iz, 'x': ix, 'y': iy}
                #k = "{layer}::{z}::{x}::{y}::{year}".format(**params)
                q = get_tile_for(params, debug)
                if (q):
                    tile = db.scalar(q)
                    #cache.set(k, tile)
                    yield b''.join([tile])

        return Response(stream_with_context(gen()), mimetype='application/x-protobuf',)


    def get_tile_for(params, debug):
        if params['z'] < limit_zoom and params['layer'] not in filters.keys():
            return None
        fltr = filters.get(params['layer'])
        if fltr and params['z'] < limit_zoom:
            params['field'] = fltr.get('field')
            params['value'] = fltr.get('value')(params['z'])
            tq = "select ohm_tile({z}, {x}, {y}, {year}, '{layer}', '{field}', {value})"
        else:
            tq = "select ohm_tile({z}, {x}, {y}, {year}, '{layer}')"
        q = tq.format(**params)
        
        if debug:
            print(q)
        return q

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

    @app.route('/relation/<relations>', methods=['GET'])
    def getRelations(relations):
        rels = relations.split('|')
        fts = []
        for rel in rels:    
            q = """
            select items.*, st_asgeojson(ST_Transform(items.geom, 4326)) as gg
            from items
            join relation_members on items.id = relation_members.item
            join relations on relations.id = relation_members.relation
            where relations.id = {rel}
            order by items.ohm_from asc
            """.format(rel=rel)
            fts.append({'rel': rel, 'itms': list(db.execute(q))})
        
        flatten = lambda l: [item for sublist in l for item in sublist]

        ret = {
            "type": "FeatureCollection",
            "features": flatten(map(out_rel_feat, fts))
        }

        ret = json.dumps(ret)
        #hk = xxhash.xxh64(ret).hexdigest()
        #rh.set(hk, ret)
        #rk.set(relations, hk)
        return ret    

    def query_to_dict(ret):
        if ret is not None:
            return [{key: value for key, value in row.items()} for row in ret if row is not None]
        else:
            return [{}]
    
    def fix_geom(x):
        x['geom'] = json.loads(x['geom'])
        return x

    @app.route('/events/<date>', methods=['GET'])
    def getEvents(date):
        d = []
        return json.dumps(d)
    
    CORS(app, resources={r"*": {"origins": "*"}})
    Gzip(app)
    return app

app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9034', debug=True, threaded=True)
