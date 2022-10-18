"""
GIS 5578, fall 2022, assignment #2

Combine and aggregate block group, population, and address point data
using a mixture of the csv module, Pandas, and GeoPandas.

A general structure is provided for you below. Make reasonable, but most
of all consistent, choices for variable names as you proceed, and comment
your work. If you write any functions, document them appropriately.
"""

import csv
import geopandas as gpd
import pandas as pd

####
# Create new CSVs with geoid and population

pop_blockgroup = []
pop_tract = []
pop_county = []

# Read csv
with open('nhgis0093_ds172_2010_blck_grp.csv', 'r') as nhgis_csv:
    csv_dict_reader = csv.DictReader(nhgis_csv)
    
    for row in csv_dict_reader:
        
            pop_blockgroup.append({'GEOID': row['STATEA']+row['COUNTYA']+row['TRACTA']+row['BLKGRPA'],'pop_blkgrp': int(row['H7V001'])})
            pop_tract.append({'GEOID': row['STATEA']+row['COUNTYA']+row['TRACTA'],'pop_tract': int(row['H7V001'])})
            pop_county.append({'GEOID': row['STATEA']+row['COUNTYA'], 'pop_county': int(row['H7V001'])})

# Write new csv
with open('pop2010_blockgroup.csv','w', newline='') as total_pop_block_group:
    fieldnames = ['GEOID', 'pop_blkgrp']
    csv_dict_writer = csv.DictWriter(total_pop_block_group, fieldnames=fieldnames)
    csv_dict_writer.writeheader()
    for value in pop_blockgroup:
        csv_dict_writer.writerow(value)

with open('pop2010_tract.csv','w', newline='') as total_pop_block_tract:
    fieldnames = ['GEOID', 'pop_tract']
    csv_dict_writer = csv.DictWriter(total_pop_block_tract, fieldnames=fieldnames)
    csv_dict_writer.writeheader()
    for value in pop_tract:
        csv_dict_writer.writerow(value)

with open('pop2010_county.csv','w', newline='') as total_pop_block_county:
    fieldnames = ['GEOID', 'pop_county']
    csv_dict_writer = csv.DictWriter(total_pop_block_county, fieldnames=fieldnames)
    csv_dict_writer.writeheader()
    for value in pop_county:
        csv_dict_writer.writerow(value)

# Load the nhgis0093_ds172_2010_blck_grp.csv file to begin.
# Output files with only geoid and total population for
# block groups, tracts, and counties. See README.md for
# full specifications.
#
# Note that there is no geoid column present in the source
# CSV, you will need to derive geoid from the columns
# available. There is a codebook file alongside the CSV
# with field descriptions. There are also links in the
# README to descriptions of Census geographies.
#
# You MUST use csv.DictWriter to produce your output
#
# Output files are (also in README)
#
#   pop2010_blockgroup.csv
#   pop2010_tract.csv
#   pop2010_county.csv

####
# Create dictionary lookup tables
#
# YOU MAY WANT TO COMPLETE THIS PRIOR TO CSV OUTPUTS
# The dictionaries you will create are NOT necessarily
# related to the CSVs you have produced. See note at end
# of this section.
#
# It's common to want to look something up by a key, e.g.
# the population for a county by geoid. In this section,
# you will create three dictionaries to support lookup
# of population by geoid at the block group, tract, and
# county levels. These dictionaries must be named
#
#   pop_county
#   pop_tract
#   pop_blockgroup
#
# At the end of this script if a user were to query or
# print a population it must produce the correct number,
# for example,
#
#     print(pop_county['27053'])
#
# will print the population of Hennepin County you found
# from aggregating block group populations.
#
# The dictionary lookups are not explicitly related to
# writing the CSVs, but you could relate them if you want
# to, not required. Do what works for you. If you want
# to learn something new you could apply here, but don't
# need to use, try out comprehensions in Python. Start
# with list comprehensions, then move into dictionary
# comprehensions. Comprehensions for tuples and sets
# comprehensions are things, too.


####
# Convert block group dictionary to a Pandas dataframe.
#

blockgroup_pop_df = pd.DataFrame.from_dict(pop_blockgroup)
blockgroup_pop_df.head()

#
# blockgroup_pop_df = pd.DataFrame.from_dict(pop_blockgroup)
#
# This will not produce what you need, you should receive
# an error. Consider the structure of the dictionary and
# what pd.DataFrame.from_dict takes for arguments.
#
# You will need the rename() and reset_index() functions,
# avaliable on the data frame, to structure the data so
# that you can later perform an attribute join onto the
# block group geometries.

####
# Load the block group shapefile into GeoPandas
#
# GeoPandas is able to load data out of a zipped shapefile.
# To save space, block groups are offered in a zipfile,
# tl_2019_27_bg.zip.
#
# If the block group file is in the same directory as your
# script the command below will load the data.

blockgroups_df = gpd.read_file("zip://tl_2019_27_bg.zip")
print(f"Loaded {len(blockgroups_df):,} block groups")

####
# Perform an attribute/tabular join
#
# Join the block group population counts from your blockgroup_pop_df
# Pandas dataframe onto your blockgroups_df dataframe. Note that the
# join operation produces a new geospatial dataframe, however you can
# replace the contents of blockgroups_df with the new geospatial
# dataframe, similar to how you might increment the value of x.
#
# When you are done with this join you will have a geospatial data
# frame with block groups including their populations from 2010.
# Use POP2010 as the field name for the population.
#
blkgrpjoin = blockgroup_pop_df.merge(blockgroups_df, on='GEOID')
blkgrpjoin.head()

####
# Perform a spatial join to count address points in block groups
#
# There are 481,609 points to load, be patient. Once the data
# is loaded you need to produce a count of the number of address points
# in each block group using a spatial join. Check the GeoPandas documentation.
# The goal is to add a column to your blockgroups_df, named ADDRPOINTS,
# that contains the count of addresses in that block group.
#

# Spatial Join
# Rectify coordinate systems
corr_addrpoints_df = addrpoints_df.to_crs(4269)
corr_addrpoints_df.crs
joined_df = gpd.sjoin(blockgroups_df, corr_addrpoints_df)
joined_df.head()

# You will run into a projection issue here. Do not reproject outside of
# GeoPandas, your final script must work with the data provided. Again,
# check the GeoPandas documentation.

addrpoints_df = gpd.read_file("zip://henn_addr_points_geom_only.zip")
print(f"Loaded {len(addrpoints_df):,} address points")

# Pro-tip: use the sample() method from [Geo]Pandas to lessen the number
# of address points you're testing with. Be sure to remove the sampling
# line (or comment it out) before sumbitting your code.
addrpoints_df = addrpoints_df.sample(10000)  # reduce to 10,000 sampled records
print(f"There are now {len(addrpoints_df):,} address points")

####
# Export a new shapefile of block groups
mnblock_group = joined_df.to_file('mnblock_group.shp')  

# Save mn_blockgroup.shp to disk. It must include the following columns
# with correct values:
#
#    GEOID
#    POP2010
#    ADDRPOINTS
#
# and have the correct block group geometries. You may leave the
# additional columns in place, but see if you can figure out how to
# get rid of them. Excessive, unnecessary data can be a real nuisance.

####
# Aggregate block groups to tracts

# Aggregate to Tract
mnblock_group_aggr = mnblock_group[['GEOID', 'pop_tract']]

# dissolve by region 
tract_aggr = mnblock_group_aggr.dissolve(by='TRACTCE')

#
# Create tract-level geometries and save a mn_tract.shp shapefile
# with the same columns as the block groups file

####
# Aggregate to counties

# Aggregate to County
county_aggr = tract_aggr.dissolve(by='COUNTYFP')

# Use either your block groups spatial dataframe or your tracts
# spatial dataframe to aggregate one more time, this time to the county
# level. Produce an mn_county.shp shapefile with the same columns as
# before.

county_aggr.to_file('mn_county.shp')

# Code Sources:   
# https://www.youtube.com/watch?v=jnkPnNaLY3g&ab_channel=AdamGaweda
# https://tutorial.eyehunts.com/python/python-dictionary-sum-values-with-the-same-key-example-     code/#:~:text=A%20list%20of%20dictionaries%20can,the%20dictionary%20must%20be%20unique.
# https://stackoverflow.com/questions/56274566/geopandas-not-able-to-change-the-crs-of-a-geopandas-object
# https://www.earthdatascience.org/workshops/gis-open-source-python/dissolve-polygons-in-python-geopandas-shapely/