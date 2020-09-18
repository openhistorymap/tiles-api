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
        Column('id', String, primary_key=True),
        Column('ohm_from', Float),
        Column('ohm_to', Float),
        Column('layer', String),
        Column('properties', JSONB),
        Column('geom', Geometry(geometry_type='GEOMETRY', srid=3857))
    )
    ohm_rels = Table('relations', metadata,
        Column('id', String, primary_key=True),
        Column('ohm_from', Float),
        Column('ohm_to', Float),
        Column('layer', String),
        Column('properties', JSONB),
        Column('members', JSONB),
        Column('geom', Geometry())
    )

    query = """
    CREATE OR REPLACE FUNCTION getmvt(zoom integer, x integer, y integer)
    RETURNS bytea AS $$
    SELECT STRING_AGG(mvtl, '') AS mvt FROM (
        SELECT IsEmpty, count(*) OVER () AS LayerCount, mvtl FROM (
            SELECT CASE zoom <= 8 
                WHEN TRUE 
                THEN FALSE 
                ELSE ST_WITHIN(ST_GeomFromText('POLYGON((0 4096,0 0,4096 0,4096 4096,0 4096))', 3857), ST_COLLECT(mvtgeometry)) END 
            AS IsEmpty, ST_AsMVT(tile, 'water', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, class, intermittent FROM layer_water(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'waterway', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", class, brunnel, intermittent FROM layer_waterway(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'landcover', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, class, subclass FROM layer_landcover(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'landuse', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, class FROM layer_landuse(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'mountain_peak', 4096, 'mvtgeometry') as mvtl FROM (SELECT osm_id, ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 64, true) AS mvtgeometry, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", class, ele, ele_ft, rank FROM layer_mountain_peak(TileBBox(zoom, x, y), zoom, 256) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 64, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'park', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, class, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", rank FROM layer_park(TileBBox(zoom, x, y), zoom, 256) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'boundary', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, admin_level, disputed, maritime FROM layer_boundary(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'aeroway', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, ref, class FROM layer_aeroway(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'transportation', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, class, subclass, oneway, ramp, brunnel, service, layer, level, indoor, surface FROM layer_transportation(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'building', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) AS mvtgeometry, render_height, render_min_height, colour, hide_3d FROM layer_building(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 4, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'water_name', 4096, 'mvtgeometry') as mvtl FROM (SELECT osm_id, ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 256, true) AS mvtgeometry, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", class, intermittent FROM layer_water_name(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 256, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'transportation_name', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 8, true) AS mvtgeometry, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", ref, ref_length, network::text, class::text, subclass, layer, level, indoor FROM layer_transportation_name(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 8, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'place', 4096, 'mvtgeometry') as mvtl FROM (SELECT osm_id, ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 256, true) AS mvtgeometry, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", class, rank, capital, iso_a2 FROM layer_place(TileBBox(zoom, x, y), zoom, 256) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 256, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'housenumber', 4096, 'mvtgeometry') as mvtl FROM (SELECT ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 8, true) AS mvtgeometry, housenumber FROM layer_housenumber(TileBBox(zoom, x, y), zoom) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 8, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'poi', 4096, 'mvtgeometry') as mvtl FROM (SELECT osm_id, ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 64, true) AS mvtgeometry, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", class, subclass, agg_stop, layer, level, indoor, rank FROM layer_poi(TileBBox(zoom, x, y), zoom, 256) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 64, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
                UNION ALL
            SELECT FALSE AS IsEmpty, ST_AsMVT(tile, 'aerodrome_label', 4096, 'mvtgeometry') as mvtl FROM (SELECT osm_id, ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 64, true) AS mvtgeometry, name, name_en, name_de, NULLIF(tags->'name:ar', '') AS "name:ar", NULLIF(tags->'name:az', '') AS "name:az", NULLIF(tags->'name:be', '') AS "name:be", NULLIF(tags->'name:bg', '') AS "name:bg", NULLIF(tags->'name:br', '') AS "name:br", NULLIF(tags->'name:bs', '') AS "name:bs", NULLIF(tags->'name:ca', '') AS "name:ca", NULLIF(tags->'name:co', '') AS "name:co", NULLIF(tags->'name:cs', '') AS "name:cs", NULLIF(tags->'name:cy', '') AS "name:cy", NULLIF(tags->'name:da', '') AS "name:da", NULLIF(tags->'name:de', '') AS "name:de", NULLIF(tags->'name:el', '') AS "name:el", NULLIF(tags->'name:en', '') AS "name:en", NULLIF(tags->'name:eo', '') AS "name:eo", NULLIF(tags->'name:es', '') AS "name:es", NULLIF(tags->'name:et', '') AS "name:et", NULLIF(tags->'name:eu', '') AS "name:eu", NULLIF(tags->'name:fi', '') AS "name:fi", NULLIF(tags->'name:fr', '') AS "name:fr", NULLIF(tags->'name:fy', '') AS "name:fy", NULLIF(tags->'name:ga', '') AS "name:ga", NULLIF(tags->'name:gd', '') AS "name:gd", NULLIF(tags->'name:he', '') AS "name:he", NULLIF(tags->'name:hr', '') AS "name:hr", NULLIF(tags->'name:hu', '') AS "name:hu", NULLIF(tags->'name:hy', '') AS "name:hy", NULLIF(tags->'name:id', '') AS "name:id", NULLIF(tags->'name:is', '') AS "name:is", NULLIF(tags->'name:it', '') AS "name:it", NULLIF(tags->'name:ja', '') AS "name:ja", NULLIF(tags->'name:ja_kana', '') AS "name:ja_kana", NULLIF(tags->'name:ja_rm', '') AS "name:ja_rm", NULLIF(tags->'name:ka', '') AS "name:ka", NULLIF(tags->'name:kk', '') AS "name:kk", NULLIF(tags->'name:kn', '') AS "name:kn", NULLIF(tags->'name:ko', '') AS "name:ko", NULLIF(tags->'name:ko_rm', '') AS "name:ko_rm", NULLIF(tags->'name:la', '') AS "name:la", NULLIF(tags->'name:lb', '') AS "name:lb", NULLIF(tags->'name:lt', '') AS "name:lt", NULLIF(tags->'name:lv', '') AS "name:lv", NULLIF(tags->'name:mk', '') AS "name:mk", NULLIF(tags->'name:mt', '') AS "name:mt", NULLIF(tags->'name:ml', '') AS "name:ml", NULLIF(tags->'name:nl', '') AS "name:nl", NULLIF(tags->'name:no', '') AS "name:no", NULLIF(tags->'name:oc', '') AS "name:oc", NULLIF(tags->'name:pl', '') AS "name:pl", NULLIF(tags->'name:pt', '') AS "name:pt", NULLIF(tags->'name:rm', '') AS "name:rm", NULLIF(tags->'name:ro', '') AS "name:ro", NULLIF(tags->'name:ru', '') AS "name:ru", NULLIF(tags->'name:sk', '') AS "name:sk", NULLIF(tags->'name:sl', '') AS "name:sl", NULLIF(tags->'name:sq', '') AS "name:sq", NULLIF(tags->'name:sr', '') AS "name:sr", NULLIF(tags->'name:sr-Latn', '') AS "name:sr-Latn", NULLIF(tags->'name:sv', '') AS "name:sv", NULLIF(tags->'name:th', '') AS "name:th", NULLIF(tags->'name:tr', '') AS "name:tr", NULLIF(tags->'name:uk', '') AS "name:uk", NULLIF(tags->'name:zh', '') AS "name:zh", NULLIF(tags->'name_int', '') AS "name_int", NULLIF(tags->'name:latin', '') AS "name:latin", NULLIF(tags->'name:nonlatin', '') AS "name:nonlatin", class, iata, icao, ele, ele_ft FROM layer_aerodrome_label (TileBBox(zoom, x, y), zoom, 256) WHERE ST_AsMVTGeom(geometry, TileBBox(zoom, x, y), 4096, 64, true) IS NOT NULL) AS tile HAVING COUNT(*) > 0
            ) AS all_layers
        ) AS counter_layers
    HAVING BOOL_AND(NOT IsEmpty OR LayerCount <> 1);
    $$ LANGUAGE SQL STABLE RETURNS NULL ON NULL INPUT;
    """

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


        layers = ['boundary', 'culture', 'transportation', 'waterway', 'water', 'building', 'industrial']
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
                ]).where(ohm_items.c.properties['layer'].astext == l)
            subq = subq.alias('q')
            qs.append(select([func.ST_AsMVT(column('q'), 'ohm', 2**12, 'geom')]).select_from(subq))

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
            id = jdata['id'],
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
