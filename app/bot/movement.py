from db import ohm_rel_members

bot = 'movement'

def run(db):
    q = """
    delete from relations
    where layer = '{}'
    and author = 'ohm'
    """.format(bot)
    db.execute(q)
    if bot == 'movement':
        q = """
        select distinct properties ->> 'name'
        from items 
        where layer = 'movement'
        """
        for r in db.execute(q):
            n = r[0]
            coll_q = """
            select *
            from items 
            where properties ->> 'name' = '{}'
            """.format(n)
            coll = list(db.execute(coll_q))
            print(coll)
            pp = """
            insert into relations (ohm_from, ohm_to, layer)
            select min(ohm_from) as ohm_from, max(ohm_to) as ohm_to, layer
            from items 
            where properties ->> 'name' = '{}'
            group by layer
            RETURNING id, ohm_from, ohm_to
            """.format(n)
            rid, _from, _to = list(db.execute(pp))[0]

            itms = []
            for ci in coll:
                    

                itms.append(dict(
                    relation = rid,
                    item = ci.id,
                    author = 'bot'
                ))
            db.execute(ohm_rel_members.insert(), itms)
