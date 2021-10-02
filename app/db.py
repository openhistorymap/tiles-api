import os
import json
import hashlib

import click

from psycopg2.pool import ThreadedConnectionPool

from sqlalchemy import DDL, create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, and_, or_, PrimaryKeyConstraint


from sqlalchemy.sql import functions, select, func, column, union
from sqlalchemy.ext.declarative import declarative_base, instrument_declarative
from sqlalchemy.sql.expression import union_all
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import sessionmaker

from sqlalchemy.dialects import postgresql
from geoalchemy2 import Geometry
from geoalchemy2.functions import GenericFunction, ST_AsMVTGeom,  ST_TileEnvelope
from functools import partial
from sqlalchemy import event

from redis import Redis

from queue import Queue, Empty
import threading
from threading import Thread



POSTGRES = os.environ.get('POSTGRES', '51.15.160.236:25432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'admin')
POSTGRES_PASS = os.environ.get('POSTGRES_PASS', 'tgZWW3Dgze94FN9O')
POSTGRES_DBNAME = os.environ.get('POSTGRES_DBNAME', 'ohm_prod')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_DB = int(os.environ.get('REDIS_DB', '3'))

OHM_EXTENSIONS = """
create extension postgis;
create extension postgis_topology;
create extension plpgsql;
"""

OHM_PREPARE_CONF__DROP = """drop table zoom_levels;"""

OHM_PREPARE_CONF__CREATE = """
create table zoom_levels(
    minzoom integer,
    maxzoom integer,
    precision integer,
    simplify boolean default true
);
"""
OHM_PREPARE_CONF__PREPARE = """
insert into zoom_levels (minzoom, maxzoom, precision, simplify) values
(0, 2, 5000, true),
(2, 4, 1250, true),
(4, 6, 300, true),
(6, 8, 80, true),
(8, 10, 20, true),
(10, 12, 5, true),
(12, 16, 0.5, true),
(16, 20, 0.25, true),
(20, 25, 1, false)
"""

OHM_PREPARE_BOTS_LOG__DROP = """drop table bots_log;"""

OHM_PREPARE_BOTS_LOG__CREATE = """
create table bots_log(
    bot text,
    ident text,
    operation text,
    op_time datetime,
    data jsonb
);
"""


OHM_PROCEDURE_STOREGEOM = """

CREATE or replace function ohm_storegeometryhash(
    geom GEOMETRY,
    minzoomin integer default 0,
    maxzoomin integer default 25
) returns text
LANGUAGE plpgsql
AS $$
    DECLARE 
        ghash TEXT;
        gtype TEXT;
        doskip INTEGER;
        z INTEGER;
        geom_simp  GEOMETRY;
        dosimp BOOLEAN;
		dozoom INTEGER;
        minz INTEGER;
        row zoom_levels;
    BEGIN 

        select md5(st_astext(geom)) into ghash;
        select count(*) from geometries where hash = ghash into doskip;
        IF doskip > 0 THEN
            return ghash;
        else
            select ST_GeometryType(ST_Force2D(geom)) into gtype;

            if gtype = 'ST_Point' then
                INSERT INTO public.geometries(hash, minzoom, zoom, geom, geom_type)
                VALUES
                    (ghash, minzoomin, maxzoomin, ST_Force2D(geom), gtype)
                on conflict(hash, zoom, geom_type) do nothing;
            else
                for z in minzoomin .. maxzoomin loop
                    select count(*) from zoom_levels where maxzoom = z into dozoom;
                    if  dozoom > 0 THEN
                        select * from zoom_levels where maxzoom = z into row;
                        if dosimp THEN
                            select st_simplifypreservetopology(ST_Force2D(geom), row.precision) into geom_simp;
                            INSERT INTO public.geometries(hash, minzoom,  zoom, geom, geom_type)
                            VALUES (ghash, row.minzoom, row.maxzoom, geom_simp, gtype)
                            on conflict(hash, zoom, geom_type) do nothing;
                        else 
                            INSERT INTO public.geometries(hash, minzoom,  zoom, geom, geom_type)
                            VALUES (ghash, row.minzoom, row.maxzoom, geom, gtype)
                            on conflict(hash, zoom, geom_type) do nothing;
                        end if;
                    end if;
                end loop;
               
            end if;
        end if;
        return ghash;
    end;
$$;



"""

OHM_PROCEDURE_MVT = """
-- DROP FUNCTION ohm_tile(integer,integer,integer,real,text,text,integer)
create or replace function ohm_tile(
	z integer,
	x integer,
	y integer,
	t real,
	lyr text,
	field text default null,
	lim integer default null
	) returns bytea
LANGUAGE plpgsql
AS $$
	 DECLARE 
	 tile bytea;
	 tfield text;
	 tlim text;
    BEGIN 
		--select field, lim from zoom_filters where layer = lyr and zoom = z into field, lim;
		select ST_AsMVT(q, lyr, 4096, 'geom', 'fid') from (
                SELECT 
                    min(items.id) as fid, 
                    items.properties, 
                    ST_AsMVTGeom(geometries.geom, ST_TileEnvelope(z, x, y), 4096, 256, true) as geom
                FROM public.items
                left join geometries on geometries.hash = items.hash
                where 
                    geometries.zoom > z AND z >= geometries.minzoom AND
                    geometries.geom && ST_TileEnvelope(z, x, y) AND
                    items.layer = lyr AND
                    items.ohm_from <= t AND ( items.ohm_to > t OR items.ohm_to is Null) and 
					CASE WHEN field is null
						THEN 1
						ELSE (properties->>field)::int
					END 
					<= 
					CASE WHEN field is null
						THEN 1
						ELSE lim
					END


                group by 
	                items.properties, geometries.geom
            ) as q into tile;
		return tile;
    end;
$$; 
"""

Base = declarative_base()

class OhmMixin:
    id = Column(Integer, autoincrement=True, nullable=False)
    #timeline = Column(String, index=True, default='default')
    ohm_from = Column(Float, index=True)
    ohm_to = Column(Float, index=True)
    layer = Column(String, index=True)
    properties = Column(JSONB)
    hash = Column(String, index=True)
    author = Column(String, default='ohm')
    


class OhmItems(OhmMixin, Base):
    __tablename__ = 'items'
    __table_args__ = (
        PrimaryKeyConstraint('id', 'layer', 'ohm_from', 'ohm_to'),
        {
            'postgresql_partition_by': 'LIST (layer)',
        }
    )

class OhmRels(OhmMixin, Base):
    __tablename__ = 'relations'
    __table_args__ = (
        PrimaryKeyConstraint('id', 'layer', 'ohm_from', 'ohm_to'),
        {
            'postgresql_partition_by': 'LIST (layer)',
        }
    )

    
class OhmGeometriesMixin:
    hash = Column(String, index=True)
    minzoom = Column(Integer, index=True, default=0)
    zoom = Column(Integer, index=True)
    geom = Column(Geometry(geometry_type='GEOMETRY', srid=3857, dimension=3))
    geom_type = Column(String)

class OhmGeometries(OhmGeometriesMixin, Base):
    __tablename__ = 'geometries'
    __table_args__ = (
        PrimaryKeyConstraint('hash', 'zoom', 'geom_type'),
        {
            'postgresql_partition_by': 'LIST (geom_type)',
        }
    )

class OhmRelMembers(Base):
    __tablename__ = 'relation_members'
    id = Column(Integer, primary_key=True, autoincrement=True)
    relation = Column(Integer, index=True)
    item = Column(Integer, index=True)
    role = Column(String, index=True)
    author = Column(String, default='ohm')


timepartitions =  {
    "mode": "range",
    "field": "ohm_from",
    "limits": [{
        "values": [-10000000000, -4000]
    }, {
        "values": [-4000, -3500]
    }, {
        "values": [-3500, -3000]
    }, {
        "values": [-3000, -2500]
    }, {
        "values": [-2500, -2000]
    }, {
        "values": [-2000, -1500]
    }, {
        "values": [-1500, -1000]
    }, {
        "values": [-1000, -500]
    }, {
        "values": [-500, -50]
    }, {
        "values": [-50, 50]
    }, {
        "values": [50, 150]
    }, {
        "values": [150, 250]
    }, {
        "values": [250, 350]
    }, {
        "values": [350, 550]
    }, {
        "values": [550, 559]
    }, {
        "values": [559, 650]
    }, {
        "values": [650, 750]
    }, {
        "values": [750, 1000]
    }, {
        "values": [1000, 1200]
    }, {
        "values": [1200, 1400]
    }, {
        "values": [1400, 1500]
    }, {
        "values": [1500, 1600]
    }, {
        "values": [1600, 1700]
    }, {
        "values": [1700, 1800]
    }, {
        "values": [1800, 1900]
    }, {
        "values": [1900, 1910]
    }, {
        "values": [1910, 1915]
    }, {
        "values": [1915, 1920]
    }, {
        "values": [1920, 1925]
    }, {
        "values": [1925, 1930]
    }, {
        "values": [1930, 1935]
    }, {
        "values": [1935, 1940]
    }, {
        "values": [1940, 1945]
    }, {
        "values": [1945, 1950]
    }, {
        "values": [1950, 1960]
    }, {
        "values": [1960, 2000]
    }, {
        "values": [2000, 3000]
    }, {
        "values": [3000, 10000000000]
    }]
}

def describe_partitions(name, typ, tname, mixin):
    return {
        "apply_to": name,
        "type": typ,
        "name": tname,
        "mixin": mixin,
        "pks": ['id', 'layer', 'ohm_from', 'ohm_to'],
        "partitions": {
            "mode": "list",
            "field": "layer", 
            "limits": [{
                "name": "boundary",
                "values":["boundary", "boundary_label"],
                "partitions": timepartitions
            }, {
                "name": "culture",
                "values":["culture", "culture_label"],
                "partitions": timepartitions
            }, {
                "name": "religion",
                "values":["religion", "religion_label"],
                "partitions": timepartitions
            }, { 
                "name": "water",
                "values":["water", "waterway", "water_label", "waterway_label"],
                "partitions": timepartitions
            }, {
                "name": "building",
                "values":["building", "building_label"],
                "partitions": timepartitions
            }, {
                "name": "place",
                "values":["place", "place_label"],
                "partitions": timepartitions
            }, {
                "name": "transportation",
                "values":["transportation", "transportation_label"],
                "partitions": timepartitions
            }, {
                "name": "landuse",
                "values":["landuse", "landuse_label"],
                "partitions": timepartitions
            }, {
                "name": "industrial",
                "values":["industrial", "industrial_label"],
                "partitions": timepartitions
            }, {
                "name": "ephemeral",
                "values": ["movement", "event", "movement_label", "event_label"],
                "partitions": timepartitions
            }]
        }
    }

setup = [
    describe_partitions("items", OhmItems, "OhmItems", OhmMixin),
    describe_partitions("relations", OhmRels, "OhmRels", OhmMixin), {
    "apply_to": "geometries",
    "type": OhmGeometries,
    "name": "OhmGeometries",
    "mixin": OhmGeometriesMixin,
    "pks": ['hash', 'zoom', 'geom_type'],
    "partitions": {
        "mode": "list",
        "field": "geom_type",
        "limits": [{
            "name": "points",
            "values": ["ST_Point"]
        }, {
            "name": "others",
            "values": ["ST_MultiPoint", "ST_MultiLineString", "ST_MultiPolygon", "ST_Polygon", "ST_LineString"],
            "partitions": {
                "mode": "range",
                "field": "zoom",
                "limits":[
                    {"values": [0, 2],}, 
                    {"values": [2, 4],}, 
                    {"values": [4, 6],}, 
                    {"values": [6, 8],}, 
                    {"values": [8, 10],}, 
                    {"values": [10, 12],}, 
                    {"values": [12, 16],}, 
                    {"values": [16, 20],}, 
                    {"values": [20, 25],}, 
                    {"values": [25, 100],}
                ]
            }
        }]
    }
}]



def create_partitions(s, parent=None):
    pd = s['partitions']
    del s['partitions']
    for lp in pd['limits']:
        pname = lp.get('name') or '{}_{}'.format(lp['values'][0], lp['values'][1])
        tn = '{}__{}'.format(parent, pname).replace('-', 'bce')
        targs = {}
        if 'partitions' in lp:
            targs = {
                'postgresql_partition_by': '{} ({})'.format(lp['partitions']['mode'].upper(), lp['partitions']['field']),
            }
        lt = type('{}_{}'.format(s['name'],pname), (s['mixin'], Base), {
            '__tablename__': tn,
            '__table_args__': (
                PrimaryKeyConstraint(*s['pks']),
                targs
            )
        })
        lt.__table__.add_is_dependent_on(s['type'].__table__)
        if pd['mode'] == 'list':
            event.listen( lt.__table__,
                "after_create",
                DDL("""ALTER TABLE {} ATTACH PARTITION {} 
                    FOR VALUES IN ('{}');""".format(parent, tn, "','".join(lp['values']))
                )
            )
        elif pd['mode'] == 'range': 
            event.listen( lt.__table__,
                "after_create",
                DDL("""ALTER TABLE {} ATTACH PARTITION {} 
                    FOR VALUES FROM ({}) TO ({});""".format(parent, tn, lp['values'][0], lp['values'][1])
                )
            )
        if "partitions" in lp:
            lp['pks'] = s['pks']
            lp['type'] = lt
            lp['name'] = pname
            lp['mixin'] = s['mixin']
            create_partitions(lp, parent=tn)
    
for s in setup:
    if "partitions" in s:
        create_partitions(s, parent=s['apply_to'])
        


@click.group()
def cli():
    pass

@click.command()
@click.option('--dbname', default=POSTGRES_DBNAME, help='database name')
def initdb(dbname):
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(
            POSTGRES_USER, POSTGRES_PASS, POSTGRES, dbname
        ), pool_size=20, max_overflow=0, pool_pre_ping=True,
        echo=False)
    db = engine.connect()
    for ex in OHM_EXTENSIONS.split('\n'):
        if len(ex) > 0:
            try:
                db.execute(ex)
            except:
                pass
    Base.metadata.create_all(engine)
    try:
        db.execute(OHM_PREPARE_CONF__DROP)
    except:
        pass
    
    db.execute(OHM_PREPARE_CONF__CREATE)
    db.execute(OHM_PREPARE_CONF__PREPARE)
    db.execute(OHM_PROCEDURE_STOREGEOM)
    db.execute(OHM_PROCEDURE_MVT)
    click.echo('Initialized the database')


class Worker(Thread):
    _TIMEOUT = 2
    """ Thread executing tasks from a given tasks queue. Thread is signalable, 
        to exit
    """
    def __init__(self, tasks, th_num):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon, self.th_num = True, th_num
        self.done = threading.Event()
        self.start()

    def run(self):       
        while not self.done.is_set():
            try:
                func, args, kwargs = self.tasks.get(block=True,
                                                   timeout=self._TIMEOUT)
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    print(e)
                finally:
                    self.tasks.task_done()
            except Empty as e:
                pass
        return

    def signal_exit(self):
        """ Signal to thread to exit """
        self.done.set()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads, tasks=[]):
        self.tasks = Queue(num_threads)
        self.workers = []
        self.done = False
        self._init_workers(num_threads)
        for task in tasks:
            self.tasks.put(task)

    def _init_workers(self, num_threads):
        for i in range(num_threads):
            self.workers.append(Worker(self.tasks, i))

    def add_task(self, func, *args, **kwargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kwargs))

    def _close_all_threads(self):
        """ Signal all threads to exit and lose the references to them """
        for workr in self.workers:
            workr.signal_exit()
        self.workers = []

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()

    def __del__(self):
        self._close_all_threads()


@click.command()
@click.option('--workers', default=20, help='number of workers')
@click.argument('names', default="bot,store")
def run(workers, names):
    qs = names.split(',')
    r = Redis(REDIS_HOST, REDIS_PORT, REDIS_DB)
    
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(
            POSTGRES_USER, POSTGRES_PASS, POSTGRES, POSTGRES_DBNAME
        ), pool_size=30, max_overflow=10, pool_pre_ping=True,
        echo=False)
    
    click.echo('running {} workers'.format(workers))
    running = 0
    tp = ThreadPool(workers)
    dbs = sessionmaker(bind = engine)
    while running <= workers*2:
        tp.add_task(worker, dbs, r, qs)
        running += 1
        
    tp.wait_completion()
    click.echo('shutting down')

import importlib

def worker(dbs, r, keys):
    while True:
        db = dbs()
        k, v = r.blpop(keys)
        if k == b'bot':
            click.echo(f'got bot {v}')
            m = importlib.import_module('bot.%s' % v.decode('utf-8'))
            m.run(db)
        if k == b'store':
            try:
                vv = json.loads(v)
                geom = vv['geom'].replace(', ', ',')
                del vv['geom']
                #hh = hashlib.md5(geom.split(';')[1].encode('utf-8')).hexdigest()
                h = db.scalar('select ohm_storegeometryhash(\'{}\')'.format(geom))
                click.echo('hash: {}'.format(h))
                vv['hash'] = h
                vv['layer'] = vv['layer'].replace('places', 'place')
                oi = OhmItems(**vv)
                db.add(oi)
                db.commit()
            except Exception as ex:
                print(ex, v)
                db = dbs()
        db.close()
            


cli.add_command(initdb)
cli.add_command(run)

if __name__ == '__main__':
    cli()