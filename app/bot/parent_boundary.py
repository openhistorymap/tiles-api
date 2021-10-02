import json

bot = 'parent'

def run(db):
    #q = """
    #delete from geometries where hash in (
    #    select hash from items where 
    #    author = 'ohmbot_{}'
    #);
    #""".format(bot)
    #db.execute(q)
    #q = """
    #delete from items where author = 'ohmbot_{}'
    #""".format(bot)
    #db.execute(q)
    q = """
    select distinct properties ->> 'wikidata' from items where author = 'ohmbot_parent' and layer = 'boundary'
    """
    rso = [x[0] for x in list(db.execute(q))]
    print(rso)
    q = """
    select distinct properties ->> 'parent_level:wikidata' from items where properties ->> 'parent_level:wikidata' not in ('{}') and layer='boundary'
    """.format("','".join(filter(lambda x: x != "", [str(r) for r in rso])))
    rs = list(db.execute(q))
    print(rs)
    for r in rs:
        if r[0] is not None:
            print (r)
            macro_parent = r[0]
            pitm = dict(list(db.execute("""select * from items where properties ->> 'parent_level:wikidata' = '{}' and layer not like '%_label' limit 1""".format(macro_parent)))[0])
            coll_q = """
            select ohm_from, ohm_to 
            from items 
            where properties ->> 'parent_level:wikidata' = '{}'
            """.format(macro_parent)

            props = {}
            for k in pitm['properties']:
                if 'parent_level' in k:
                    props[k.replace('parent_level:', '', 1)] = pitm['properties'][k]

            coll = list(db.execute(coll_q))
            ocoll = []
            for c in coll:
                ocoll.append(c[0])
                ocoll.append(c[1])
            ocoll = list(set(ocoll))
            ocoll = sorted(ocoll)
            print(len(ocoll))
            for i in range(len(ocoll)-1):
                ohm_from = ocoll[i]
                ohm_to = ocoll[i+1]
                pms = dict(
                    fr=ohm_from, 
                    to=ohm_to, 
                    layer = pitm['layer'], 
                    id=macro_parent, 
                    properties = json.dumps(props), 
                    bot = bot
                )
                pp = """

                insert
                into items (ohm_from, ohm_to, layer, hash, properties, author)
                select {fr}, {to}, '{layer}', ohm_storegeometryhash(st_union(geoms)), '{properties}', 'ohmbot_{bot}'
                from (
                    select geom as geoms from geometries where zoom > 20
                    and hash in (
                            select distinct(hash)
                            from items
                            where properties->>'parent_level:wikidata' = '{id}'
                            and ohm_from <= {fr}
                            and  ohm_to >= {to}
                            and layer = '{layer}'
                        ) ) as  gg
                """.format(**pms)
                print(pms)
                print(pp)
                db.execute(pp)

                pp = """

                insert
                into items (ohm_from, ohm_to, layer, hash, properties, author)
                select {fr}, {to}, '{layer}_label', ohm_storegeometryhash(ST_Centroid(st_union(geoms))), '{properties}', 'ohmbot_{bot}'
                from (
                    select geom as geoms from geometries where zoom > 20
                    and hash in (
                            select distinct(hash)
                            from items
                            where properties->>'parent_level:wikidata' = '{id}'
                            and ohm_from <= {fr}
                            and  ohm_to >= {to}
                            and layer = '{layer}'
                        ) ) as  gg
                """.format(**pms)
                print(pms)
                print(pp)
                db.execute(pp)
                db.commit()
    db.close()

