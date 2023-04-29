"""
The L1 optimizer class
"""

from solvar.core.utils import *
from solvar.optimizer.grbOptimizer import GrbOptimizer
from solvar.solvar.histogram import getKeysForHistogram
from collections import defaultdict
import gurobipy

class L1Optimizer(GrbOptimizer):
    """
        Assumes arbitrary schema. Solves for histogram_b:

                    max sum_{b in blocks} L1(histogram_b, initialHistogram_b)

                subject to

                    block-level histograms match 2010 public block-level counts
                    tract-level histograms match 2010 public tract-level counts
    """
    def __init__(self, name:str="l1", **kwargs) -> None:
        super().__init__(name=name, **kwargs)

    def buildModel(self) -> None:
        self.info(f"Building l1-norm model {self.name}...")
        self.setModelParams(name=f"grb_recon_{self.name}")
        
        self.model.Params.TimeLimit = self.gurobiTimeout
        self.info(f"Beginning to optimize initial model (with timeout = {self.gurobiTimeout} seconds) ...")
        self.model.optimize()
        self.info(f"Finished optimizing initial model. Status was {self.model.Status}.")
        if self.checkTimeout():
            return
        
        variables = self.model.getVars()
        self.keyToVars, self.rhsSums = getKeysForHistogram(variables, self.initialValues, self.schema, self.VAR_COMPONENTS)
        
        self.info("Adding sum variables and constraints...")
        self.addSumVarsAndCons()    # add grb vars & cons to represent histogram counts using keyToVars
        
        self.info("Adding absolute value variables and constraints...")
        self.addAbsValVarsAndCons() # add grb vars & cons to model maximizing L1 norm compared to sumVars
        
        self.info(f"Finished building l1-norm model {self.name}")
