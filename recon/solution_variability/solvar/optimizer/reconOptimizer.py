"""
The recon optimizer class
"""

from solvar.core.utils import *
from solvar.optimizer.grbOptimizer import GrbOptimizer
import gurobipy

class ReconOptimizer(GrbOptimizer):
    """
        Assumes arbitrary schema. Solves for reconstruction solution:

                    max 0

                subject to

                    block-level histograms match 2010 public block-level counts
                    tract-level histograms match 2010 public tract-level counts
    """
    def __init__(self, name:str="ReconOptimizer", **kwargs) -> None:
        super().__init__(name=name, **kwargs)

    def buildModel(self) -> None:
        raise Exception("Reconstruction optimizer doesn't build a new model, just re-uses Tammy .lp file.")
