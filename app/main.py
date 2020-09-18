from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float
from sqlalchemy.sql import functions, select, func, column, union
from sqlalchemy.sql.expression import union_all
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.orm.session import Session
from geoalchemy2 import Geometry
from geoalchemy2.functions import GenericFunction, ST_AsMVTGeom,  ST_TileEnvelope
import mercantile
from functools import partial
import pyproj
from shapely.ops import transform

from shapely.geometry import shape

import os
import json

from flask import Flask
from flask import request, make_response
from flask import jsonify

from flask_cors import CORS

POSTGRES = os.environ.get('POSTGRES', '51.15.160.236:25432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'admin')
POSTGRES_PASS = os.environ.get('POSTGRES_PASS', 'tgZWW3Dgze94FN9O')
POSTGRES_DBNAME = os.environ.get('POSTGRES_DBNAME', 'ohm')

TILES_LAYERS=os.environ.get('TILES_LAYERS', 'boundary,culture,religious,transportation,waterway,water,building,industrial').split(',')

class ST_AsMVT(GenericFunction):
    type = BYTEA



def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
      
    engine = create_engine(
        'postgresql://{}:{}@{}/{}'.format(
            POSTGRES_USER, POSTGRES_PASS, POSTGRES, POSTGRES_DBNAME
        ), 
        echo=True)
    db = engine.connect()

    metadata = MetaData()
    ohm_items = Table('items', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('ohm_from', Float),
        Column('ohm_to', Float),
        Column('layer', String),
        Column('properties', JSONB),
        Column('geom', Geometry(geometry_type='GEOMETRY', srid=3857))
    )
    ohm_rels = Table('relations', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('ohm_from', Float),
        Column('ohm_to', Float),
        Column('layer', String),
        Column('properties', JSONB),
        Column('members', JSONB),
        Column('geom', Geometry())
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

    @app.route('/items/<year>/<z>/<y>/<x>/vector.pbf')
    def properties(year,z,y,x):
        layers = TILES_LAYERS
        ymd = request.args.get('d')
        xmin, ymin, xmax, ymax = mercantile.xy_bounds(mercantile.Tile(z = int(z), x = int(x),y = int(y)))

        qs = []
        out = []
        for l in layers:
            subq = select([
                column('id'),
                column('properties'),
                func.ST_AsMVTGeom(ohm_items.c.geom, 
                    func.ST_MakeBox2D(func.ST_Point(xmin, ymin),
                                    func.ST_Point(xmax, ymax)),
                    2**12,
                    2**8,
                    True
                    ).label('geom'),
                ]).where(ohm_items.c.layer == l).alias('q')
            qs.append(select([func.ST_AsMVT(column('q'), l, 2**12, 'geom')]).select_from(subq))

        ua = union(*qs)
        tile = db.scalar(ua)

        response = make_response(tile)
        response.headers['Content-Type'] = "application/x-protobuf"
        return response


    def map_single(jdata):
        ss = shape(jdata['geometry'])
        pdata = jdata['properties']
        print(jdata)
        return dict(
            ohm_from = jdata['from'],
            ohm_to = jdata['to'],
            layer = pdata['layer'],
            properties = pdata,
            geom = 'SRID=3857;' + transform(project, ss).wkt
        )


    @app.route('/items', methods=['POST'])
    def saveItem():
        data = request.data
        jdata = json.loads(data)
        if isinstance(jdata, dict):
            jdata = [jdata]
        jdata = map(map_single, jdata)
        db.execute(ohm_items.insert(), jdata)

        return jsonify({'result': 'OK'})

    CORS(app, resources={r"*": {"origins": "*"}})
    return app

app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9034', debug=True)
