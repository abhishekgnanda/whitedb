import gevent.monkey; gevent.monkey.patch_all()
import gevent
import wgdb

d_raw = wgdb.attach_database('20000', 20000000)


def wg_get(i, db = None, key = ''):
    #print "s3 " + str(i)
    if not db or not key:
        return None
    
    result = None
    l = None
    q = None
    try:
        #print "s4 " + str(i)
        l = wgdb.start_read(db)
        #print "s5 " + str(i)
        q = wgdb.make_query(db, arglist=[(0, wgdb.COND_EQUAL, str(key))])
        #print "s6 " + str(i)
        if q.res_count:
            #print "s7 " + str(i)
            rec = wgdb.fetch(db, q)
            #print "s8 " + str(i)
            result = wgdb.get_field(db, rec, 1)
            #print "s9 " + str(i)
        wgdb.end_read(db, l)
        #print "s0 " + str(i)
        wgdb.free_query(db, q)
        #print "sr " + str(i)
    except:
        print 'wg_get exception: %s, %s' %(l, q)
        if l:
            wgdb.end_read(db, l)
        if q:
            wgdb.free_query(db, q)
        return None
    
    return result


def get_trd(i):
    #print "s1 " + str(i)
    results = []
    while True:
        #print "s2 " + str(i)
        results = wg_get(i, d_raw, '001')


g_list = []
for i in xrange(10):
    g = gevent.spawn(get_trd, i)
    g_list.append(g)
    
gevent.joinall(g_list)
