import os
import json
import traceback
import mercantile
from functools import partial
import pyproj
from shapely.ops import transform
from shapely.geometry import shape, MultiPolygon, MultiPoint, MultiLineString
import numpy
from redis import Redis
from pydantic import BaseModel

from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, Union,List

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_DB = int(os.environ.get('REDIS_DB', '3'))

EXPANDER = os.environ.get('EXPANDER', 'https://github.com/openhistorymap/mapstyles/blob/master/expander.json')

EPHEMERAL_STEP = float(1)/12/31
PERSIST_STEP = float(1)/12



def get_month(x, year_step = EPHEMERAL_STEP):
    yr = [int(x)+y for y in [x*year_step for x in range(0,12*31)]]
    nyr = sorted(yr + [x])
    i = nyr.index(x)
    return yr[i-1]

def float_to_date(f):
    y = int(f)
    m = int((f-y)*12)+1
    d = int(((f-y)*12-(m-1))*30)
    return '{}-{}-{}'.format(y,m,d)

def create_app(test_config=None):
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    r = Redis(REDIS_HOST, REDIS_PORT, REDIS_DB)

    project = partial(
        pyproj.transform,
        pyproj.Proj(init='epsg:4326'), # source coordinate system
        pyproj.Proj(init='epsg:3857')) # destination coordinate system

    @app.get('/')
    def index():
        return ''

    def map_single(jdata):
        if jdata['geometry'] is None:
            return None
        ss = shape(jdata['geometry'])
        ss = transform(project, ss)
        pdata = jdata['properties']
        #print(pdata)
        _from = jdata['from'] if 'from' in jdata else pdata['ohm:from:date']
        _to = jdata['to'] if 'to' in jdata else pdata['ohm:to:date']
        cs = [ss]
        if isinstance(ss, (MultiPolygon, MultiLineString, MultiPoint,)):
            cs = [its.buffer(0) for its in ss if its]
        ret = []
        pdata['$area'] = ss.area
        pdata['$length'] = ss.length
        if 'Poly' in ss.geom_type:
            try:
                ret.append(dict(
                    ohm_from = _from,
                    ohm_to = _to,
                    layer = pdata['layer']+"_label",
                    properties = pdata,
                    geom = 'SRID=3857;' + ss.representative_point().wkt
                ))
            except: 
                print('error_label', ss.wkt)
        for s in cs:
            try: 
                ret.append(dict(
                    ohm_from = _from,
                    ohm_to = _to,
                    layer = pdata['layer'],
                    properties = pdata,
                    geom = 'SRID=3857;' + ss.wkt
                ))
            except:
                print('error', ss.wkt)
        return ret

    class OHMServiceItemsResponse(BaseModel):
        result:str
        items_added: int

    class OHMErrorResponse(BaseModel):
        result:str
        error:str
        stack:str

    class GeoJ(BaseModel):
        properties: Any


    class JData(BaseModel):
        itms: List[GeoJ]
        rel: str

    @app.post('/items', response_model=Union[OHMServiceItemsResponse, OHMErrorResponse])
    def saveItem(jdata: Union[List[JData],JData] = Body(...)):
        try:
            if not isinstance(jdata, list):
                jdata = [jdata]
            print(jdata)
            jdata = list(map(map_single, jdata))
            #print(len(jdata))

            flat_list = []
            for sublist in jdata:
                if sublist:
                    for item in sublist:
                        if item:
                            flat_list.append(item)
            if len(flat_list) > 0:
                r.rpush('store', *[json.dumps(fi) for fi in flat_list])
            return {'result': 'OK', 'items_added': len(flat_list)}
        except Exception as ex:
            print(ex)
            return {'result': 'error', 'error': traceback.format_exception_only(type(ex), ex), 'stack':traceback.format_stack()}

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
     
    class LoadingStatus(BaseModel):
        store: int

    class OHMServiceLoadingStatusResponse(BaseModel):
        result:str
        status: LoadingStatus

        
    @app.get('/status', response_model=OHMServiceLoadingStatusResponse)
    def status():
        ret = {
            'store': r.llen('store')
        }
        return {'result': 'OK', 'status': ret}
        
    return app
    return app

app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9039', debug=True, threaded=True)
