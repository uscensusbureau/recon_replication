"""
Intrinsic Variability Functions
"""

from solvar.core.utils import *
from solvar.core.base import Solvar
from solvar.optimizer.l1Optimizer import L1Optimizer   # grbOptimizer used to find maximally distant histogram in L1 sense
from solvar.optimizer.emdOptimizer import EmdOptimizer  # grbOptimizer used to find maximally distant histogram in Earth Mover's Distance sense
from solvar.optimizer.reconOptimizer import ReconOptimizer # grbOptimizer used to perform initial reconstruction based on published tabulations
import time

import gurobipy, numpy, sys
from collections import defaultdict

# Primary Author: Philip Leclerc
# Email: philip.leclerc@census.gov
# Last Major Update: 11/27/2018 -- updated by Galois in July 2021
#
# This script estimates the 'intrinsic variability' in a reconstruction model,
#   & generates estimates of the effect of sampling zeros on intrinsic variability.
#
# Script usage:
#
#   python intrinsic_variability.py baseInputFile relaxedInputFile outputFileBase tractPop mode
#
#   -- baseInputFile should be a .lp file generated according to Tammy's reconstruction methodology & formatting
#       The included file loving_texas_example.lp is an example of an appropriate .lp file.
#
#   -- outputFileBase is an arbitrary file path, with directories on the path assumed to already exist. Logging output
#       will be generated and deposited in outputFileBase. Conventional suffix is .out.
#
#   -- tractPop is the target tract's population, used for output statistics
#
#   NOTE: considerable additional output directed to STDOUT currently. User may find it useful to redirect STDOUT to
#       a second file.

class IntrinsicVariability(Solvar):
    def __init__(self, logger:Optional[logging.Logger]=None) -> None:
        if (logger):
            self.logger = logger #overwrite default

    def estimateAgeIntrinsicVariability(self, *, baseModel:gurobipy.Model, outputFileBase:Path=None) -> Union[str,float]:
        """
            Compute L1[(B),(6)] : Age EMD Intrinsic Variability
        """
        start_t = time.time()
        schema = self.AGE_SCHEMA if (not self.useImpreciseAgeBuckets) else self.AGE_SCHEMA_IMPRECISE_AGE_BUCKETS

        baseReconOptimizer = ReconOptimizer(name="IV_ageReconOptimizer", model=baseModel,
                                            schema=schema, outputFileBase=outputFileBase,
                                            logger=self.logger)

        baseReconOptimizer.optimize()
        if baseReconOptimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        initialValues : defaultdict = defaultdict(lambda: 0)
        for var in baseReconOptimizer.model.getVars():
            initialValues[var.VarName] = var.X

        maxEMDAgeOptimizer = EmdOptimizer(name="IV_ageEmdOptimizer", model=baseModel,
                                        schema=schema, outputFileBase=outputFileBase,
                                        maxAge=self.SF1_2010_MAX_AGE, initialValues=initialValues,
                                        logger=self.logger)

        maxEMDAgeOptimizer.buildModel()
        if maxEMDAgeOptimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        maxEMDAgeOptimizer.optimize()
        if maxEMDAgeOptimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        assert baseReconOptimizer.solution, "[AgeIV]: reconOpt solution is not set!"
        assert maxEMDAgeOptimizer.solution, "[AgeIV]: emdOpt solution is not set!"
        self.info(f"Computing L1 distance between {maxEMDAgeOptimizer.model.ModelName} solution histogram and baseModel solution histogram:") #this is not returned, but will show up in the log file
        l1 = maxEMDAgeOptimizer.l1Dist(maxEMDAgeOptimizer.solution.histogram, baseReconOptimizer.solution.histogram)

        self.info(f"Computing Earth Mover's Distance distance between {maxEMDAgeOptimizer.model.ModelName} solution histogram and baseModel solution histogram:")
        grbEmd = maxEMDAgeOptimizer.model.ObjVal
        emd = maxEMDAgeOptimizer.emdDist(maxEMDAgeOptimizer.solution.histogram, baseReconOptimizer.solution.histogram)
        if not self.useImpreciseAgeBuckets:
            assert numpy.isclose([emd], [grbEmd]), "grbEmd != emd: " + str(grbEmd) + " vs " + str(emd)

        # compute solve time
        end_t = time.time()
        self.info(f"[AgeIV] Solve Time: {(end_t - start_t):.5f} seconds")

        return emd

    def estimateDemoIntrinsicVariability(self, *, baseModel:gurobipy.Model, outputFileBase:Path=None) -> Union[str,float]:
        """
            Compute L1[(A),(4)] : Demographics L1 Intrinsic Variability
        """
        start_t = time.time()

        baseReconOptimizer = ReconOptimizer(name="IV_demoReconOptimizer", model=baseModel,
                                            schema=self.DEMO_SCHEMA, outputFileBase=outputFileBase,
                                            logger=self.logger)

        baseReconOptimizer.optimize()
        if baseReconOptimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        initialValues : defaultdict = defaultdict(lambda: 0)
        for var in baseReconOptimizer.model.getVars():
            initialValues[var.VarName] = var.X

        maxL1Optimizer = L1Optimizer(name="IV_demoL1Optimizer", model=baseModel,
                                    schema=self.DEMO_SCHEMA, outputFileBase=outputFileBase,
                                    maxAge=self.SF1_2010_MAX_AGE, initialValues=initialValues,
                                    logger=self.logger)

        maxL1Optimizer.buildModel()
        if maxL1Optimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        maxL1Optimizer.optimize()
        if maxL1Optimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        assert baseReconOptimizer.solution, "[DemoIV]: reconOpt solution is not set!"
        assert maxL1Optimizer.solution, "[DemoIV]: l1Opt solution is not set!"
        self.info(f"Computing L1 distance between {maxL1Optimizer.model.ModelName} solution histogram and baseModel solution histogram:")
        l1 = maxL1Optimizer.l1Dist(maxL1Optimizer.solution.histogram, baseReconOptimizer.solution.histogram)

        # compute solve time
        end_t = time.time()
        self.info(f"[DemoIV] Solve Time: {(end_t - start_t):.5f} seconds")

        return l1

    def estimateIntrinsicVariability(self, *, baseModel:gurobipy.Model, outputFileBase:Path=None) -> Union[str,float]:
        """
            Compute L1[(1),(2)] : L1 Intrinsic Variability
        """
        start_t = time.time()

        schema = self.DEFAULT_SCHEMA if (not self.useImpreciseAgeBuckets) else self.DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS

        baseReconOptimizer = ReconOptimizer(outputFileBase=outputFileBase, schema=schema,
                                            name="IV_reconOptimizer", model=baseModel,
                                            logger=self.logger)

        baseReconOptimizer.optimize()
        if baseReconOptimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        initialValues : defaultdict = defaultdict(lambda: 0)
        for var in baseReconOptimizer.model.getVars():
            initialValues[var.VarName] = var.X

        maxL1Optimizer = L1Optimizer(name="IV_L1Optimizer", model=baseModel,
                                    schema=schema, outputFileBase=outputFileBase,
                                    maxAge=self.SF1_2010_MAX_AGE, initialValues=initialValues,
                                    logger=self.logger)

        maxL1Optimizer.buildModel()
        if maxL1Optimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        maxL1Optimizer.optimize()
        if maxL1Optimizer.optimizationTimeout:
            return self.TIMEOUT_RETURN_VALUE

        assert baseReconOptimizer.solution, "[IV]: reconOpt solution is not set!"
        assert maxL1Optimizer.solution, "[IV]: l1Opt solution is not set!"
        self.info(f"Computing L1 distance between {maxL1Optimizer.model.ModelName} solution histogram and baseModel solution histogram:")
        l1 = maxL1Optimizer.l1Dist(maxL1Optimizer.solution.histogram, baseReconOptimizer.solution.histogram)

        # compute solve time
        end_t = time.time()
        self.info(f"[IV] Solve Time: {(end_t - start_t):.5f} seconds")

        return l1

    def getEstimates(self, baseInputFile:Path, outputFileBase:Path, grbLogFile:Path) -> Dict[str,Union[str,float]]:
        """
            primary code entry point; derives estimates based on 3 models & their solutions:

           (0) main model              (reproduce Tammy solution, up to minor gurobi non-determinism)
           (1) main L1 model           (max L1 dist hist, age as unordered)
           (2) demo L1 model           (max L1 dist demo x blocks sub-hist)
           (3) age EMD model           (max EMD dist age x blocks sub-hist)
        """
        self.info(f"\n\n-------------------------------------------------")
        self.info(f"Initiating intrinsic variability analysis for {baseInputFile}.")
        self.info(f"Output spam can be found in {outputFileBase}")

        grbEnv = gurobipy.Env.OtherEnv(grbLogFile.as_posix(), "Census", "DAS", 0, "") # type: ignore  ## the stub seems incorrect
        baseModel : gurobipy.Model = gurobipy.read(baseInputFile.as_posix(), env=grbEnv)
        
        import socket
        self.info(f"Gurobi logfile {socket.gethostname()} creating logfile {grbLogFile}")

        variability_estimates = {}  # dict to store estimated effects

        self.info(f"Initiating calculation of basic intrinsic variability.")
        variability_estimates["IV"] = self.estimateIntrinsicVariability(baseModel=baseModel.copy(), outputFileBase=outputFileBase)
        self.info(f"Finished calculation of basic intrinsic variability.")

        if self.computeDemoIV:
            self.info(f"Initiating calculation of demo intrinsic variability.")
            variability_estimates["Demo IV"] = self.estimateDemoIntrinsicVariability(baseModel=baseModel.copy(), outputFileBase=outputFileBase)
            self.info(f"Finished calculation of demo intrinsic variability.")

        if self.computeAgeIV:
            self.info(f"Initiating calculation of age intrinsic variability.")
            variability_estimates["Age IV"] = self.estimateAgeIntrinsicVariability(baseModel=baseModel.copy(), outputFileBase=outputFileBase)
            self.info(f"Finished calculation of age intrinsic variability.")

        self.info(f"Intrinsic variability analysis for {baseInputFile} returned estimates:")

        for estimateName in variability_estimates.keys():
            self.info(f"{estimateName} = {variability_estimates[estimateName]}")
            if variability_estimates[estimateName] != self.TIMEOUT_RETURN_VALUE:
                assert variability_estimates[estimateName] >= 0.0, str(estimateName) + " cannot be negative (but it was!)." # type: ignore

        self.info(f"Intrinsic variability analysis done for {baseInputFile}. Closing up shop...")

        return variability_estimates
