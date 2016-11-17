"""
isoblue topology
"""

from streamparse import Grouping, Topology

from spouts.ibengSpout import IbengSpout 
from spouts.ibimpSpout import IbimpSpout 

from bolts.isobusBolt import IsobusBolt
from bolts.engineRpmBolt import EngineRpmBolt
from bolts.ptoRpmBolt import PtoRpmBolt

class isoblue(Topology):

    ibeng_spout = IbengSpout.spec()
    ibimp_spout = IbimpSpout.spec()

    isobus_bolt = IsobusBolt.spec(inputs=[ibeng_spout, ibimp_spout])
    engrpm_bolt = EngineRpmBolt.spec(inputs={isobus_bolt['engrpm']: Grouping.SHUFFLE})
    ptorpm_bolt = PtoRpmBolt.spec(inputs={isobus_bolt['ptorpm']: Grouping.SHUFFLE})
