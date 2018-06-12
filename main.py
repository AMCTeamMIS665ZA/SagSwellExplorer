#Geographic projection packages
import pyproj
import stateplane

#Numeric and data manipulation
import numpy as np
import pandas as pd
#import dask.dataframe as dd

#Visualization packages
import holoviews as hv
import geoviews as gv
from datashader import utils as ut

from colorcet import cm
from bokeh.models import WMTSTileSource
from holoviews.streams import RangeXY, PlotSize
from holoviews.operation.datashader import datashade, aggregate, shade, dynspread
from holoviews import streams
import datashader.transfer_functions as tf
import datashader as ds
from holoviews.operation import decimate
decimate.max_samples=100
from bokeh.models import HoverTool

# Utility packages
import os
import glob
#import datetime as dt
import time as tm

import win32api

#Parameterization tools for interactive widgets
import param, paramnb, parambokeh

from IPython.display import HTML

hv.extension('bokeh', logo=False)


#Load Data
df = pd.read_csv('./data/SagSwellDataRedux.csv')
print(len(df))
df.head(2)

####   Setting Some Options   ####
# Specify initial X,Y range for points to be plotted.
x_range, y_range = ((-10975134.269,-10512411.001), (4414961.940,4896987.468))

# Specify options for the basemap size
options = dict(width=640,height=380,xaxis=None,yaxis=None,bgcolor='black',show_grid=False )

#Define tile service to be used as base map for the points
tiles = gv.WMTS(WMTSTileSource(url='https://maps.wikimedia.org/osm-intl/{Z}/{X}/{Y}@2x.png'))


####  Class Definition   ####
class SagSwellExplorer(hv.streams.Stream):
    
    alpha       = param.Magnitude(default=0.75, doc="Alpha value for the map opacity")
    plot        = param.ObjectSelector(default="Sag", objects=["Sag","Swell"])
    colormap    = param.ObjectSelector(default=cm["fire"], objects=cm.values())
    numEvents   = param.Range(default=(1, 300), bounds=(1, 300), doc="""Filter for event count""")
    ByDay    = param.Boolean(False, doc="Filter By Day")
    DayNum      = param.Integer(1,bounds=(1,15)) # Stop at 15 since that's all the data we have loaded
    ByFeeder    = param.Boolean(False, doc="Filter By Feeder")
    Feeder      = param.ObjectSelector(default="28GM012002", objects=df.FEEDER_ID.sort_values().unique().tolist())
    BySUB    = param.Boolean(False, doc="Filter By SUB")
    Substations = param.ObjectSelector(default="28GM", objects=df.SUB.sort_values().unique().tolist())
    maxpix      =      param.Integer(12)
    threshhold = param.Number(0.6,bounds=(0.1,1.0))
    

    def make_view(self, x_range=None, y_range=None, **kwargs):
        #map_tiles = tiles.opts(style=dict(alpha=self.alpha), plot=options) 
        
        points = hv.Points(df, kdims=['X_CORD', 'Y_CORD'], vdims=['EVENT_COUNT','EventType','SUB','day','FEEDER_ID'])

        if (self.BySUB & self.ByFeeder & self.ByDay) :
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations,day=self.DayNum,FEEDER_ID=self.Feeder )
        elif ( self.BySUB & self.ByFeeder & (not self.ByDay)):
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations,FEEDER_ID=self.Feeder )
        elif (self.BySUB & (not self.ByFeeder) & self.ByDay):
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations,day=self.DayNum )
        elif (self.BySUB & (not self.ByFeeder) & (not self.ByDay)):
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations)
        elif ((not self.BySUB) & self.ByFeeder & self.ByDay):
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents,day=self.DayNum,FEEDER_ID=self.Feeder )
        elif ((not self.BySUB) & self.ByFeeder & (not self.ByDay)):
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents,FEEDER_ID=self.Feeder )
        elif ((not self.BySUB) & (not self.ByFeeder) & self.ByDay):
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents,day=self.DayNum )
        else:
            selected = points.select(EventType=self.plot,EVENT_COUNT=self.numEvents)
        
        SagSwellPts = datashade(selected, x_sampling=1, y_sampling=1, cmap=self.colormap,
                               dynamic=False, x_range=x_range, y_range=y_range,width=640,height=380)
        dsss = dynspread(SagSwellPts,shape='circle',max_px=self.maxpix,threshold=self.threshhold)
        #return map_tiles * dsss
        return dsss


    def jtdp(self, x_range, y_range, **kwargs):
        
        pointdec = hv.Points(df, kdims=['X_CORD', 'Y_CORD'], vdims=['EVENT_COUNT','FEEDER_ID'])
        selecteddec = pointdec.select(EventType=self.plot,EVENT_COUNT=self.numEvents )        
        dm2 = decimate(selecteddec,x_range=x_range, y_range=y_range, dynamic=False).opts(style={'Points': dict(alpha=0.0,color='blue',size=self.maxpix )})
        return dm2
    
    def dec_tab(self, x_range, y_range, bounds, **kwargs):
        
        #%opts Table [ fig_size=550 width=600 height=380]
        
        b0=bounds[0]; b2=bounds[2]; b1=bounds[1]; b3=bounds[3]
        
        xr = bounds[2]-bounds[0]; yr = bounds[3]-bounds[1]
        
        if ( not ((xr < 50000) & (yr < 50000) )) :
            b0=b2=b1=b3=0.0
            win32api.MessageBox(0,"SELECTED AREA TOO LARGE! ",'dec_tab',0x00001000)
        
        pointdec = hv.Points(df, kdims=['X_CORD', 'Y_CORD'], vdims=['EVENT_COUNT','EventType','SUB','day','FEEDER_ID','XFMR','Phase'])

        if (self.BySUB & self.ByFeeder & self.ByDay) :
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations,day=self.DayNum,FEEDER_ID=self.Feeder )
        elif ( self.BySUB & self.ByFeeder & (not self.ByDay)):
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations,FEEDER_ID=self.Feeder )
        elif (self.BySUB & (not self.ByFeeder) & self.ByDay):
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations,day=self.DayNum )
        elif (self.BySUB & (not self.ByFeeder) & (not self.ByDay)):
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents,SUB=self.Substations)
        elif ((not self.BySUB) & self.ByFeeder & self.ByDay):
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents,day=self.DayNum,FEEDER_ID=self.Feeder )
        elif ((not self.BySUB) & self.ByFeeder & (not self.ByDay)):
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents,FEEDER_ID=self.Feeder )
        elif ((not self.BySUB) & (not self.ByFeeder) & self.ByDay):
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents,day=self.DayNum )
        else:
            selected = pointdec.select(X_CORD=(b0, b2),Y_CORD=(b1, b3),EventType=self.plot,EVENT_COUNT=self.numEvents)
        
        #bp=selected.select( X_CORD=(b0, b2),Y_CORD=(b1, b3)  )
        tab = hv.Table(selected,kdims=[],vdims=['EventType','SUB','FEEDER_ID','XFMR','Phase'])
        
        return tab
   


# Map Tiles
map_tiles = tiles.opts(style=dict(alpha=1), plot=options) 

# Create an explorer object and start visualizing data
explorer = SagSwellExplorer()
#pbk=parambokeh.Widgets(explorer,view_position='right', callback=explorer.event)

# Declare points selection
mv = hv.DynamicMap(explorer.make_view, streams=[explorer, RangeXY()])

# For Box Select
box = streams.BoundsXY(source=mv, bounds=(0,0,0,0))
# For Table to display points selected with box select
dt = hv.DynamicMap(explorer.dec_tab, streams=[explorer, RangeXY(), box])


#plot = hv.renderer('bokeh').get_plot(mv*map_tiles+dt, doc=Document())

plot = hv.renderer('bokeh').instance(mode='server').get_plot(mv*map_tiles+dt)
#plot = hv.renderer('bokeh').instance(mode='server').get_plot(mv*map_tiles)

parambokeh.Widgets(explorer,view_position='right', callback=explorer.event, plots=[plot.state],mode='server')








