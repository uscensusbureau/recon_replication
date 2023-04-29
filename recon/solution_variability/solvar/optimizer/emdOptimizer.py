"""
The EMD optimizer class
"""

from solvar.core.utils import *
from solvar.optimizer.grbOptimizer import GrbOptimizer
import numpy
from collections import defaultdict
import gurobipy

class EmdOptimizer(GrbOptimizer):
    """
        Assumes Block x Age schema. Solves for histogram_b:

                    max sum_{b in blocks} EMD(histogram_b, initialHistogram_b)

                subject to

                    block-level histograms match 2010 public block-level counts
                    tract-level histograms match 2010 public tract-level counts
    """
    def __init__(self, name:str="emd", **kwargs) -> None:
        super().__init__(name=name, **kwargs)

    def buildModel(self) -> None:
        self.info(f"Building Earth Mover's Distance model {self.name}...")
        self.setModelParams(name=f"grb_recon_{self.name}")

        self.model.Params.TimeLimit = self.gurobiTimeout
        self.info(f"Beginning to optimize initial model (with timeout = {self.gurobiTimeout} seconds) ...")
        self.model.optimize()
        self.info(f"Finished optimizing initial model. Status was {self.model.Status}.")
        if self.checkTimeout():
            return

        self.baseVariables = self.model.getVars()

        self.info(f"Constructing age to variables CDF...")
        self.getAgeToVarsCDF()

        self.info(f"Adding sum variables and constraints...")
        self.addSumVarsAndCons()    # Adds constraints as well

        self.info(f"Adding absolute value variables and constraints...")
        self.addAbsValVarsAndCons() # Adds constraints & obj fxn coeffs as well

        self.info(f"Finished building Earth Mover's Distance model {self.name}")
        opt_model_name = f"{self.outputFileBase.name}_{self.name}_emd_model.lp"
        print(f"writing {opt_model_name}...")
        self.model.write(str(opt_model_name))

    def getAgeToVarsCDF(self) -> None:
        """
            get a dict that maps (block, age) into vars that sum over ages 0, 1, ..., age
            and get initial solution values for these as well
        """
        varBlockIndex = self.VAR_COMPONENTS.index("blockGeocode")
        varAgeIndex = self.VAR_COMPONENTS.index("age")
        nonemptyBlocks = list(set([v.VarName.split('_')[varBlockIndex] for v in self.baseVariables if v.VarName.startswith("C_")]))
        assert self.maxAge is not None, "EmdOptimizer requires a maximum age (assumed schema: Block x Age)."
        cdfBlockAgeToVars_arr = numpy.full([len(nonemptyBlocks)*(self.maxAge+1), len(self.baseVariables)], False, dtype=bool) # type: ignore
        cdfInitial_arr = numpy.zeros(len(nonemptyBlocks)*(self.maxAge+1), dtype=int) # type: ignore

        for i,v in enumerate(self.baseVariables): # Build var sets / counts for possibly non-zero variables
            varName = v.VarName
            #if 'dummy' not in varName and ('male' in varName or 'female' in varName): # Ensure this is a relevant variable
            #if self.initialValues[varName] > 0.0:
            #    self.info("varName in getAgeToVarsCDF: ", varName, "with >0 value ", self.initialValues[varName], " & isclose ", numpy.isclose(self.initialValues[varName], 1.0))
            if varName.startswith("C_"):
                varValues = varName.split('_')
                block = varValues[varBlockIndex]
                age = int(varValues[varAgeIndex])

                blockIndex = nonemptyBlocks.index(block)
                cdfBlockAgeToVars_arr[blockIndex*(self.maxAge+1)+age : (blockIndex+1)*(self.maxAge+1), i] = True
                if varName in self.initialValues and numpy.isclose([self.initialValues[varName]], [1.0]):
                    cdfInitial_arr[blockIndex*(self.maxAge+1)+age : (blockIndex+1)*(self.maxAge+1)] += 1

        baseVariables_arr = numpy.asarray(self.baseVariables) # type: ignore
        # the following 2 variables can be just dicts, but need to be defaultdicts due to type checking
        self.keyToVars : defaultdict = defaultdict(lambda: set(), {f'{block}_{a}' : set(baseVariables_arr[cdfBlockAgeToVars_arr[blockIndex*(self.maxAge+1)+a]]) for a in range(self.maxAge+1) for blockIndex, block in enumerate(nonemptyBlocks)}) # Every block-age combo should get a variable in grbOptimizer
        self.rhsSums : defaultdict = defaultdict(lambda: 0, {f'{block}_{a}' : cdfInitial_arr[blockIndex*(self.maxAge+1)+a] for a in range(self.maxAge+1) for blockIndex, block in enumerate(nonemptyBlocks)})
