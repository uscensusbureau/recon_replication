#!/usr/bin/env /home/simsong/anaconda3/bin/python3
# -*- coding: utf-8 -*-
"""
This is a CGI script that I ran on DreamHost to report the status of the reconstruction at Cornell.
(The Cornell reconstruction was coordinated using a MySQL server on Dreamhost.)
This should be transistioned to the DAS dashboard.
"""


import re
import cgitb
import os
import sys
import datetime
import time
cgitb.enable()

sys.path.append( os.path.join(os.path.dirname(__file__),".."))

import dbrecon
import ctools.tydoc as tydoc

queries = [
    ('z','Current Time', 'select now()'),
    ('a',"Total states in database:",   "select count(distinct state) from tracts"),
    ('b',"Total counties in database:", "select count(distinct state,county) from tracts"),
    ('c',"Total tracts in database:",   "select count(*) from tracts"),
    ('d',"Number of LP files created in past hour:", "select count(*) from tracts where unix_timestamp() - unix_timestamp(lp_end) < 3600"),
    ('e',"Number of SOL files created in past hour:", "select count(*) from tracts where unix_timestamp() - unix_timestamp(sol_end) < 3600"),
    ('g',"LP Files created",             "SELECT sum(if(lp_end is not Null,1,0)) FROM tracts"),
    ('gg',"LP Files needed",             "SELECT sum(if(lp_end is Null,1,0)) FROM tracts"),
    ('h',"Solutions created",            "SELECT sum(if(sol_end is not Null,1,0)) FROM tracts"),
    ('hh',"Solutions Needed",            "SELECT sum(if(sol_end is Null,1,0)) FROM tracts"),
    (None, "LP files ready to solve",    "SELECT count(*) from tracts where lp_end is not null and sol_start is null"),
    (None,None,None),
    (None,"Current system load",
         "select sysload.host,sysload.t,timestampdiff(second,sysload.t,now()) as age,"
         "freegb,min1,min5,min15 from sysload inner join (select max(t) as t,host from sysload group by host) tops on sysload.t=tops.t and sysload.host=tops.host having age<600"),
    (None,"Completion of LP files at current rate:", "time.asctime(time.localtime(time.time()+int(3600*vals['gg']/vals['d']))) if vals['d']>0 else 'n/a'"),
    (None,"Completion of solutions at current rate:", "time.asctime(time.localtime(time.time()+int(3600*vals['hh']/vals['e']))) if vals['e']>0 else 'n/a'"),
    #('f',"Total completed LP and SOL files per state",
    #"select state,count(distinct county),count(*),sum(if(lp_end is not null,1,0)),sum(if(sol_end is not null,1,0)) from tracts group by state"),
    ('i',"Last 5 completed LP files:",  "SELECT state,county,tract,lp_end,timestampdiff(second,modified_at,now()) as age, timestampdiff(second,lp_start,lp_end) as secs from tracts where lp_end IS NOT NULL order by lp_end DESC limit 5"),
    ('j',"Last 5 completed SOL files:", "SELECT state,county,tract,sol_end,timestampdiff(second,modified_at,now()) as age,timestampdiff(second,sol_start,sol_end) as secs from tracts where lp_end IS NOT NULL order by sol_end DESC limit 5"),
    (None,None,None),
    (None,
         'Gurobi runs that took the most time',
    'select state,county,tract,final_pop,timestampdiff(second,sol_start,sol_end) as diff,sol_gb,sol_start '
    'from tracts having diff is not null order by diff desc limit 10'),
    (None,
         'Gurobi runs that took the most memory',
         'select state,county,tract,final_pop,timestampdiff(second,sol_start,sol_end) as diff,sol_gb,sol_start  '
         'from tracts where sol_gb is not null order by sol_gb desc limit 10'),
    (None,None,None),
    (None,"SOLs in progress",     "SELECT state,county,tract,sol_start,timestampdiff(second,sol_start,now()) as age,hostlock from tracts where sol_start is not null and sol_end is null order by sol_start"),
    (None,"LP files in progress", "SELECT state,county,tract,lp_start,timestampdiff(second,lp_start,now()) as age,hostlock from tracts where lp_start is not null and lp_end is null order by hostlock,lp_start"),
    (None,None,None),
    ]

def fmt(r):
    if isinstance(r,int):
        return "{:,}".format(r)
    if isinstance(r,datetime.datetime):
        timestr = (r - datetime.timedelta(hours=4)).isoformat()
        timestr = timestr.replace("T"," ")
    return str(r)

def clean():
    dbrecon.DB.csfr("delete from sysload where timestampdiff(second,sysload.t,now()) > 3600")
    #dbrecon.DB.csfr("update tracts set lp_start=NULL,hostlock=NULL where lp_end is null and timestampdiff(second,lp_start,now()) > 3600")
    print("cleaned.")
    exit(0)

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Maintains a database of reconstruction and schedule "
                             "next work if the CPU load and memory use is not too high." ) 
    parser.add_argument("--clean", action='store_true')
    args   = parser.parse_args()

    dbrecon.get_pw()
    config = dbrecon.get_config(filename="config.ini")

    if args.clean:
        clean()

    print("Content-Type: text/html;charset=utf-8\r\n\r\n")

    doc = tydoc.tydoc()
    doc.head.add_tag('meta',attrib={'http-equiv':'refresh','content':'30'})
    doc.body.add_tag_text('p',"Report on reconstruction. All times in GMT.")

    table = tydoc.tytable()
    doc.body.append(table)
    vals = {}
    db = dbrecon.DB()
    db.connect()
    c = db.cursor()
    for (label,desc,query) in queries:
        if desc is None:
            table = tydoc.tytable()
            doc.body.append(table)
            continue
            
        if not query.upper().startswith('SELECT'):
            table.add_data([desc,str(eval(query))])
            continue
        c.execute(query)
        rows = c.fetchall()
        for row in rows:
            if desc: # Beginning of a new query
                d = desc
                desc = None
                if len(c.description)==1 and len(rows)==1:
                    v = row[0]
                    vals[label] = v
                    table.add_data([d]+[fmt(v)])
                    continue
                else:
                    table.add_data([d])   #should span
                    table.add_data([r[0] for r in c.description])
            table.add_data([fmt(r) for r in row])
    doc.render(sys.stdout , format='html')

    
    
   
