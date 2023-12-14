'''
Program to assess suppression rates on PL-94 tables,
using DOJ-based race categories
'''

import pandas as pd
import numpy as np

def r1d(d):
    from decimal import Decimal
    one_after = round(Decimal(d), 1)
    return one_after

def eldiv(num, denom):
    '''Function to help avoid warnings due to division of large numbers'''
    if num == 0:
        return np.divide(num, denom)
    return np.exp(np.log(num) - np.log(denom))

def count_format(count):
    return(f'{count:,}')

def geo_format(geo):
    if 'block' in geo: return('Block')
    if 'blkgrp' in geo: return('Block Group')
    if 'tract' in geo: return('Tract')
    if 'county' in geo: return('County')
    if 'state' in geo: return('State')
    return('National')


FORMAT_DICT = {'Geography': geo_format,
               'Total Populated': count_format,
               'Population Meets Criteria': count_format,
               'Total Cells': count_format,
               'Cells Changed to Zero': count_format,
               'Total Tables': count_format,
               'Suppressed Tables': count_format
               }
#Hierarchies of geography to consider.
#In this application, we consider only the spine, not the ribs.
GEOLEVELS = [['nation'],
             ['nation', 'state'],
             ['nation', 'state', 'county'],
             ['nation', 'state', 'county', 'tract'],
             ['nation', 'state', 'county', 'tract', 'blkgrp'],
             ['nation', 'state', 'county', 'tract', 'blkgrp', 'block']]

#def tabulate(pcef_path, hcef_path, geolevels=GEOLEVELS):
def tabulate(pcef_path, geolevels=GEOLEVELS):

    #Read in CEF data prepared by build_cef.sas and extract.sas.
    #Store as Pandas DF with multiindex on full spine geo.
    pcef = pd.read_csv(pcef_path).set_index(geolevels[-1])
#    hcef = pd.read_csv(hcef_path).set_index(geolevels[-1])

    dfsf1 = pd.DataFrame()
    geo_list = []
    total_list = []
    cells_list = []
    pct_list = []

    #First compute geo sizes to test > 0 & < 14
    TABVARS = []
    for geo in geolevels:
        print(geo)
        tabPop = pcef.pivot_table(index=geo,
                                  columns=TABVARS, aggfunc='size', fill_value=0)
        #suppression if geo pop is > 0 and < 15
        res = (tabPop > 0) & (tabPop < 15)
        #baseline of geo pop > 0
        base = tabPop > 0
        print("Pop demo suppression geos:", base.sum(),
              res.sum(), r1d(100*eldiv(res.sum(), base.sum())))
        geo_list.append(geo)
        total_list.append(base.sum())
        cells_list.append(res.sum())
        pct_list.append(r1d(100*eldiv(res.sum(), base.sum())))

    dfsf1['Geography'] = geo_list
    dfsf1['Total Populated'] = total_list
    dfsf1['Population Meets Criteria'] = cells_list
    dfsf1['Percentage Meets Criteria'] = pct_list
    dfsf1.to_latex('suppression_sf1_tables.tex', index=False,
            formatters=FORMAT_DICT,
            caption="Geographies Meeting Criteria for Person Table Suppression in 2010 Summary File 1")
#        tabHU = hcef.query('occupied==1').pivot_table(index=geo,
#                                                      columns=TABVARS,
#                                                      aggfunc='size', fill_value=0)
#        #suppression if geo HUs are > 0 and < 5
#        res = (tabHU > 0) & (tabHU < 5)
#        #baseline of geo HU > 0
#        base = (tabHU > 0)
#        print("HU suppression geos:", base.sum(), res.sum(),
#              r1d(100*eldiv(res.sum(), base.sum())))

    dfp1 = pd.DataFrame()
    geo_list = []
    total_list = []
    cells_list = []
    pct_list = []

    #Table P1
    #No table suppressions, since this is a race-only table
    #Will calculate number of 1-2 count cells suppressed
    TABVARS = ['race']
    print('Table P1')
    for geo in geolevels:
        print(geo)
        p1 = pcef.pivot_table(index=geo,
                              columns=TABVARS, aggfunc='size', fill_value=0)
        cellsSuppressed = ((p1 < 3) & (p1 > 0)).values.sum()
        totalCells = (p1 > -1).values.sum()
        print("Cell Suppression:", totalCells, cellsSuppressed,
              r1d(100*eldiv(cellsSuppressed, totalCells)))
        geo_list.append(geo)
        total_list.append(totalCells)
        cells_list.append(cellsSuppressed)
        pct_list.append(r1d(100*eldiv(cellsSuppressed, totalCells)))
    dfp1['Geography'] = geo_list
    dfp1['Total Cells'] = total_list
    dfp1['Cells Changed to Zero'] = cells_list
    dfp1['Percent of Cells Changed'] = pct_list
    dfp1.to_latex('suppression_p1_cells.tex', index=False,
            formatters=FORMAT_DICT,
            caption="Primary Cell Suppression for 2010 P.L. 94-171 Table P1: Race")

    dfp2 = pd.DataFrame()
    geo_list = []
    total_list = []
    cells_list = []
    pct_list = []

    #Table P2
    #No table suppressions, since this is a table of his x race
    #Will calculate number of 1-2 count cells suppressed
    TABVARS = ['his', 'race']
    print('Table P2')
    for geo in geolevels:
        print(geo)
        p2 = pcef.pivot_table(index=geo,
                              columns=TABVARS, aggfunc='size', fill_value=0)
        cellsSuppressed = ((p2 < 3) & (p2 > 0)).values.sum()
        totalCells = (p2 > -1).values.sum()
        print("Cell Suppression:", totalCells, cellsSuppressed,
              r1d(100*eldiv(cellsSuppressed, totalCells)))
        geo_list.append(geo)
        total_list.append(totalCells)
        cells_list.append(cellsSuppressed)
        pct_list.append(r1d(100*eldiv(cellsSuppressed, totalCells)))
    dfp2['Geography'] = geo_list
    dfp2['Total Cells'] = total_list
    dfp2['Cells Changed to Zero'] = cells_list
    dfp2['Percent of Cells Changed'] = pct_list
    dfp2.to_latex('suppression_p2_cells.tex', index=False,
            formatters=FORMAT_DICT,
            caption="Primary Cell Suppression for 2010 P.L. 94-171 Table P2: Hispanic or Latino, and Not Hispanic or Latino by Race")

    #Table P3
    #Table suppression due to age-based margin
    #Require 15+ people PER race group
    #Will also calculate number of 1-2 count cells suppressed in remaining tables
    #Note that total number of tables may be lower than the total number of geos
    # when there are geos (blocks) that have no one over 18.
    TABVARS = ['race']
    QUERY = 'age18 == 1'
    print('Table P3')

    dfp3c = pd.DataFrame()
    dfp3t = pd.DataFrame()
    cgeo_list = []
    ctotal_list = []
    ccells_list = []
    cpct_list = []
    tgeo_list = []
    ttotal_list = []
    tcells_list = []
    tpct_list = []

    for geo in geolevels:
        print(geo)
        #Criteria for keeping the over/under 18 table is the geo-level race counts
        #Note this is just P1
        p3criteria = pcef.pivot_table(index=geo,
                                      columns=TABVARS,
                                      aggfunc='size', fill_value=0)
        p3 = pcef.query(QUERY).pivot_table(index=geo,
                                           columns=TABVARS,
                                           aggfunc='size', fill_value=0)
        keepTabs = (p3criteria[p3criteria > 0].min(axis=1) < 15)
        #kept p3 tables based on criteria
        p3keep = p3[p3.index.isin(keepTabs[keepTabs].index)]
        tabsSuppressed = p3keep.shape[0]
        tabsTotal = p3.shape[0]
        print("Table Suppression: ", tabsTotal, tabsSuppressed,
              r1d(100*eldiv(tabsSuppressed, tabsTotal)))
        tgeo_list.append(geo)
        ttotal_list.append(tabsTotal)
        tcells_list.append(tabsSuppressed)
        tpct_list.append(r1d(100*eldiv(tabsSuppressed, tabsTotal)))
        cellsSuppressed = ((p3 < 3) & (p3 > 0)).values.sum()
        totalCells = (p3 > -1).values.sum()
        print("Cell Suppression:", totalCells, cellsSuppressed,
              r1d(100*eldiv(cellsSuppressed, totalCells)))
        cgeo_list.append(geo)
        ctotal_list.append(totalCells)
        ccells_list.append(cellsSuppressed)
        cpct_list.append(r1d(100*eldiv(cellsSuppressed, totalCells)))
    dfp3c['Geography'] = cgeo_list
    dfp3c['Total Cells'] = ctotal_list
    dfp3c['Cells Changed to Zero'] = ccells_list
    dfp3c['Percent of Cells Changed'] = cpct_list
    dfp3c.to_latex('suppression_p3_cells.tex', index=False,
            formatters=FORMAT_DICT,
            caption="Primary Cell Suppression for 2010 P.L. 94-171 Table P3: Race For The Population 18 Years and Over")
    dfp3t['Geography'] = tgeo_list
    dfp3t['Total Tables'] = ttotal_list
    dfp3t['Suppressed Tables'] = tcells_list
    dfp3t['Percent of Tables Suppressed'] = tpct_list
    dfp3t.to_latex('suppression_p3_tables.tex', index=False,
            formatters=FORMAT_DICT,
            caption="Primary Table Suppression for 2010 P.L. 94-171 Table P3: Race For The Population 18 Years and Over")

    #Table P4
    #Table suppression due to age-based margin
    #Require 15+ people PER his x race group
    #Will also calculate number of 1-2 count cells suppressed in remaining tables
    #Note that total number of tables may be lower than the total number of geos
    # when there are geos (blocks) that have no one over 18.

    dfp4c = pd.DataFrame()
    dfp4t = pd.DataFrame()
    cgeo_list = []
    ctotal_list = []
    ccells_list = []
    cpct_list = []
    tgeo_list = []
    ttotal_list = []
    tcells_list = []
    tpct_list = []

    TABVARS = ['his', 'race']
    QUERY = 'age18 == 1'
    print('Table P4')
    for geo in geolevels:
        print(geo)
        #Criteria for keeping the over/under 18 table is the geo-level race counts
        #Note this is just P2
        p4criteria = pcef.pivot_table(index=geo,
                                      columns=TABVARS,
                                      aggfunc='size', fill_value=0)
        p4 = pcef.query(QUERY).pivot_table(index=geo,
                                           columns=TABVARS,
                                           aggfunc='size', fill_value=0)
        keepTabs = (p4criteria[p4criteria > 0].min(axis=1) < 15)
        #kept p4 tables based on criteria
        p4keep = p4[p4.index.isin(keepTabs[keepTabs].index)]
        tabsSuppressed = p4keep.shape[0]
        tabsTotal = p4.shape[0]
        print("Table Suppression: ", tabsTotal, tabsSuppressed,
              r1d(100*eldiv(tabsSuppressed, tabsTotal)))
        tgeo_list.append(geo)
        ttotal_list.append(tabsTotal)
        tcells_list.append(tabsSuppressed)
        tpct_list.append(r1d(100*eldiv(tabsSuppressed, tabsTotal)))
        cellsSuppressed = ((p4 < 3) & (p4 > 0)).values.sum()
        totalCells = (p4 > -1).values.sum()
        print("Cell Suppression:", totalCells, cellsSuppressed,
              r1d(100*eldiv(cellsSuppressed, totalCells)))
        cgeo_list.append(geo)
        ctotal_list.append(totalCells)
        ccells_list.append(cellsSuppressed)
        cpct_list.append(r1d(100*eldiv(cellsSuppressed, totalCells)))
    dfp4c['Geography'] = cgeo_list
    dfp4c['Total Cells'] = ctotal_list
    dfp4c['Cells Changed to Zero'] = ccells_list
    dfp4c['Percent of Cells Changed'] = cpct_list
    dfp4c.to_latex('suppression_p4_cells.tex', index=False,
            formatters=FORMAT_DICT,
            caption="Primary Cell Suppression for 2010 P.L. 94-171 Table P4: Race For The Population 18 Years and Over")
    dfp4t['Geography'] = tgeo_list
    dfp4t['Total Tables'] = ttotal_list
    dfp4t['Suppressed Tables'] = tcells_list
    dfp4t['Percent of Tables Suppressed'] = tpct_list
    dfp4t.to_latex('suppression_p4_tables.tex', index=False,
            formatters=FORMAT_DICT,
            caption="Primary Table Suppression for 2010 P.L. 94-171 Table P4: Hispanic or Latino, and Not Hispanic or Latino by Race for the Population 18 Years and Over")

#    #Table H1
#    #No table suppressions, since this is a table of occupancy
#    #Will calculate number of 1-2 count cells suppressed
#    TABVARS = ['occupied']
#    print('Table H1')
#    for geo in geolevels:
#        print(geo)
#        h1 = hcef.pivot_table(index=geo,
#                              columns=TABVARS, aggfunc='size', fill_value=0)
#        cellsSuppressed = ((h1 < 3) & (h1 > 0)).values.sum()
#        totalCells = (h1 > -1).values.sum()
#        print("Cell Suppression (remaining tables): ", totalCells,
#              cellsSuppressed, r1d(100*eldiv(cellsSuppressed, totalCells)))

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('pcef')
#    parser.add_argument('hcef')

    args = parser.parse_args()
#    tabulate(args.pcef, args.hcef)
    tabulate(args.pcef)
