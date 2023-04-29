"""
The main grbOptimizer class
"""

import gurobipy
from solvar.optimizer.optimizer import Optimizer
from solvar.solvar.histogram import Histogram
from collections import defaultdict
from solvar.core.base import Solvar
from solvar.core.utils import *

class GrbOptimizer(Optimizer):
    """
        General class for Gurobi-based optimization. Currently .optimize makes
        the specialized assumption that ordered dicts will be used to store
        solutions as histograms. This assumption is specific to the intrinsic
        variability code (i.e., .optimize should be moved to a new subclass
        if intrinsic variability and das_decennial code based are merged).
    """

    def __init__(self, outputFileBase:Path, *, model:gurobipy.Model, schema:List[str],
                name:str="generic", maxAge:Optional[int]=None,
                initialValues:Optional[defaultdict]=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.grbEnv         = None
        self.schema         = schema
        self.solution : Optional[Histogram] = None
        self.model : gurobipy.Model = model
        self.outputFileBase = outputFileBase
        self.initialValues : defaultdict = initialValues if (initialValues is not None) else defaultdict(lambda: 0)

        self.keyToVars : Optional[defaultdict] = None
        self.sumVars : Optional[List[gurobipy.Var]] = None
        self.maxAge = maxAge        # maxAge is needed in l1 to upper bound bigM (move to superclass?)
        self.rhsSums : Optional[defaultdict] = None
        self.optimizationTimeout : bool = False

    def setModelParams(self, name:Optional[str]=None) -> None:
        self.model.Params.Threads = int(self.THREADS)
        self.model.Params.MIPGap = float(self.MIP_GAP)
        self.model.ModelName = name if name is not None else "grbModelNameNotProvided"

    def optimize(self, sense:Optional[int]=None) -> None:
        self.model.ModelSense = gurobipy.GRB.MAXIMIZE if sense is None else sense
        if self.WRITE_VARIABILITY_LPS:
            self.model.write(f"{self.outputFileBase}_{self.model.ModelName}_{self.schema}.lp")

        self.model.Params.TimeLimit = self.gurobiTimeout
        self.info(f"Beginning to optimize model {self.model.ModelName} (with timeout = {self.gurobiTimeout} seconds) ...")
        self.model.optimize()
        self.info(f"Finished optimizing model {self.model.ModelName}. Status was {self.model.Status}.")
        if self.checkTimeout():
            return
        try:
            assert self.model.Status == gurobipy.GRB.OPTIMAL, f"Model {self.model.ModelName} returned non-optimal solve status {self.model.Status}"
        except:
            pass

        self.info(f"Storing self.solution as an ordered-dictionary histogram...")
        self.solution = Histogram(schema=self.schema, outputFileBase=self.outputFileBase)
        self.solution.buildHistogramFromModel(self.model)

        self.info("Total # of ppl in histogram (should equal # of ppl in reference tract's American Fact Finder total pop tab): ")
        self.info(" << computed by L1-comparing to all-0's histogram >> ")
        self.info(f"{self.l1Dist(self.solution.histogram, defaultdict(lambda: 0))}")

    def checkTimeout(self) -> bool:
        if self.model.Status == gurobipy.GRB.TIME_LIMIT:
            self.warning("Optimization timed out")
            self.optimizationTimeout = True
        return self.optimizationTimeout

    def addSumVarsAndCons(self) -> None:
        """
            add variables representing age CDF, e.g. var for ppl of age a for a in {0,1,...,115}
            and constraint a = sum_{v in V} b_v for binary vars b_v corresponding to persons of age a

            (more specific than other methods but used for all recon sol variability grb models)
        """
        sumVars = []
        assert (self.keyToVars is not None), "[GrbOptimizer.addSumVarsAndCons]: keysToVars is not set!"
        for key in self.keyToVars.keys(): # keyToVars is set in relevant sub-class (i.e.; emd,  l1)
            #cdfVar = self.model.addVar(vtype=gurobipy.GRB.CONTINUOUS, name="cdf_"+key)
            cdfVar = self.model.addVar(vtype=gurobipy.GRB.INTEGER, name="cdf_"+key)
            LHS = gurobipy.LinExpr()
            LHS += cdfVar
            for var in self.keyToVars[key]:
                LHS -= var
            self.model.addConstr(lhs=LHS, sense=gurobipy.GRB.EQUAL, rhs=0.0, name="cdf_con_"+key) # type: ignore ## stubs are different from implementation
            sumVars.append(cdfVar)
        self.model.update()
        self.sumVars = sumVars

    def addAbsValVarsAndCons(self) -> None:
        """
            abs val cons look like:

            (x - c) + bigM * B      >= absX
            (c - x) + bigM * ( 1-B) >= absX
            x - c                   <= absX
            c - x                   <= absX

            (more specific than other methods but used for all recon sol variability grb models)
        """
        # NOTE: would like to split this into separate variable / constraints sub-methods, but extra bookkeeping...
        assert self.maxAge is not None, "[addAbsValVarsAndCons]: maxAge has to be set!"
        assert self.rhsSums is not None, "[addAbsValVarsAndCons]: rhsSums has to be set!"
        assert self.sumVars is not None, "[addAbsValVarsAndCons]: sumVars has to be set!"
        bigM = (self.maxAge+1)*2*max(self.rhsSums.values())
        for sumVar in self.sumVars:
            key = '_'.join(sumVar.VarName.split('_')[1:]) # drops leading 'sum_' / 'cdf_' etc to get variable key
            rhsSum = self.rhsSums[key]
            #absVar = self.model.addVar(vtype=gurobipy.GRB.INTEGER, name="absVar_"+sumVar.VarName, obj=1.0) # type: ignore ## stubs are different from implementation
            absVar = self.model.addVar(vtype=gurobipy.GRB.CONTINUOUS, name="absVar_"+sumVar.VarName, obj=1.0)
            absSwitch = self.model.addVar(vtype=gurobipy.GRB.BINARY, name="absSwitchVar_"+sumVar.VarName)
            linExpr1 = gurobipy.LinExpr()
            linExpr2 = gurobipy.LinExpr()
            linExpr3 = gurobipy.LinExpr()
            linExpr4 = gurobipy.LinExpr()
            linExpr1 = sumVar - rhsSum + bigM * absSwitch - absVar
            linExpr2 = rhsSum - sumVar + bigM - bigM * absSwitch - absVar
            linExpr3 = sumVar - rhsSum - absVar
            linExpr4 = rhsSum - sumVar - absVar
            self.model.addConstr(lhs=linExpr1, sense='>', rhs=0.0, name="absCon1_"+sumVar.VarName) # type: ignore ## stubs are different from implementation
            self.model.addConstr(lhs=linExpr2, sense='>', rhs=0.0, name="absCon2_"+sumVar.VarName) # type: ignore ## stubs are different from implementation
            self.model.addConstr(lhs=linExpr3, sense='<', rhs=0.0, name="absCon3_"+sumVar.VarName) # type: ignore ## stubs are different from implementation
            self.model.addConstr(lhs=linExpr4, sense='<', rhs=0.0, name="absCon4_"+sumVar.VarName) # type: ignore ## stubs are different from implementation
        self.model.update()
