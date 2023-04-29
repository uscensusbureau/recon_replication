"""
The base class for optimizers
"""

from solvar.core.utils import *
from solvar.core.base import Solvar
from solvar.solvar.histogram import Histogram

from collections import defaultdict
import numpy as np
import gurobipy

class Optimizer(Solvar):

    def __init__(self, logger:Optional[logging.Logger]=None) -> None:
        self.model : Optional[gurobipy.Model] = None
        self.variables = None
        self.objFxn = None
        self.constraints = None
        self.solution : Optional[Histogram] = None

        if (logger):
            self.logger = logger #overwrite default

    def buildModel(self) -> None:
        # Default buildModel, override if model is weird
        self.buildVars()
        self.buildObjFxn()
        self.buildConstraints()
        assert self.model, "[Optimizer.buildModel]: model is not set!"

    def buildVars(self) -> None:
        # implemented in subclass
        pass

    def buildObjFxn(self) -> None:
        # implemented in subclass
        pass

    def buildConstraints(self) -> None:
        # implemented in subclass
        pass

    def optimize(self) -> None:
        # implemented in subclass / sets self.solution
        pass

    def l1Dist(self, histogram1:Optional[defaultdict], histogram2:Optional[defaultdict], showDiffs:bool=True) -> float:
        """
               L O n e D i s t a n c e

            Given two histograms with arbitrary (but identical) schemas,
            computes L1 difference between their .histogram attributes.
        """
        assert histogram1 is not None and histogram2 is not None, "Cannot compute l1Dist(hist1, hist2) over NoneTypes."
        dist = 0.0
        keys = set(histogram1.keys()).union(set(histogram2.keys()))

        self.info("NOTE: only displaying histogram counts that differ.")
        for key in keys:
            dist += abs(histogram1[key] - histogram2[key])
            if showDiffs:
                if abs(histogram1[key] - histogram2[key]) > 0.0:
                    self.info(f"VarName: {key} :: {histogram1[key]} vs {histogram2[key]}")

        if np.isclose([dist],  [0.0]):
            self.info("No differences between input histograms.")

        return dist

    # Metrics_norm functions

    def emdDist(self, histogram1:Optional[defaultdict], histogram2:Optional[defaultdict]) -> float:
        """
                E a r t h  M o v e r ' s D i s t a n c e

            Given 2 defaultdict histograms w/ keys of form

                block_age

            returns emdDist(histogram1, histogram2).

            Uses property that emdDist(x,y) = l1Dist(prefix(x), prefix(y))
            where prefix(x) is the prefixSum transformation of x.

            (This is valid when x is 1-dimensional, contains
            nonnegative counts, and x, y have identical known
            totals.)
        """
        assert histogram1 is not None and histogram2 is not None, "Cannot compute emdDist(hist1, hist2) over NoneTypes."
        #self.info(f"metric_norms.emdDist received histogram1: {histogram1}")
        #self.info(f"metric_norms.emdDist received histogram2: {histogram2}")
        prefixSums1 = self.makePrefixSums(histogram1)
        prefixSums2 = self.makePrefixSums(histogram2)
        #self.info(f"prefixSums1: {prefixSums1}")
        #self.info(f"prefixSums2: {prefixSums2}")
        prefixPairs = zip(prefixSums1, prefixSums2)
        #self.info("abs(prefixSum1 - prefixSum2):")
        #for prefixSum1, prefixSum2 in prefixPairs:
        #    self.info([(i,a) for i, a in enumerate(abs(prefixSum1 - prefixSum2))])
        #prefixPairs = zip(prefixSums1, prefixSums2)
        EMDs = [np.sum(abs(prefixSum1 - prefixSum2)) for prefixSum1, prefixSum2 in prefixPairs]
        #self.info("EMDs: ")
        #self.info(EMDs)
        return sum(EMDs)

    def makePrefixSums(self, histogram:defaultdict) -> List:
        """
            [[Helper fxn for emdDist.]]

            Given a histogram represented as a default dict, assumes keys are of the form

                block_age

            generates one prefixSum np.array per block (i.e., counts of # ppl <= age in block
            for each age).
        """
        keys = list(histogram.keys())
        assert keys[0].count('_') == 1, "Histogram key has wrong schematic length. Example: " + keys[0]
        splitKeys = [key.split('_') for key in keys]
        blocks = sorted(set([splitKey[0] for splitKey in splitKeys]))
        #self.info("blocks in makePrefixSums:")
        #for block in blocks:
        #    self.info(block)
        #ages = sorted(set([splitKey[1] for splitKey in splitKeys]))
        if (self.useImpreciseAgeBuckets):
            ages = [f"{age}.0" for age in self.AGE_BUCKETS]
        else: # precise ages
            ages = [str(i) for i in range(self.SF1_2010_MAX_AGE+1)]
        counts : List = [[] for block in blocks]
        for i, block in enumerate(blocks):
            #self.info(f"building countList for block {block}")
            for age in ages:
                counts[i].append(histogram[block + '_' + age])
        #self.info("countLists:")
        #for countList in counts:
        #    self.info([(i, c) for i, c in enumerate(countList)])
        prefixSums = [np.cumsum(countList) for countList in counts]
        #self.info("prefixSums:")
        #for prefixSum in prefixSums:
        #    self.info([(i, p) for i, p in enumerate(prefixSum)])
        return prefixSums

