from __future__ import annotations
import gurobipy
import numpy
from collections import defaultdict

from solvar.core.base import Solvar
from solvar.core.utils import *

class Histogram(Solvar):
    """
        Class for representing and manipulating histograms as ordered dictionaries of
        string (name) -> integer (count) mappings.
    """

    def __init__(self, schema:List[str], outputFileBase:Path) -> None:
        self.schema = schema                       # list for converting grb var names to equiv classes
        self.histogram : Optional[defaultdict] = None                      # ordered str -> int dict
        self.outputFile = outputFileBase.with_suffix('.out')  # str / ouput file path
        self.outputFileBase = outputFileBase       # str / output file path (w/o suffix)

    def buildHistogramFromModel(self, model:gurobipy.Model) -> None:
        assert model.Status == gurobipy.GRB.Status.OPTIMAL, "Can't build histogram for GRB model " + model.name + " with non-optimal status " + model.Status

        hist : defaultdict = defaultdict(lambda: 0)
        for variable in model.getVars():
            #if variable.X > 0 and 'dummy' not in varName and 'sum' not in varName and 'abs' not in varName and 'cdf' not in varName:
            #if variable.X > 0 and varName[:2] == "C_":
            if variable.VarName[:2] == "C_" and numpy.isclose([variable.X], [1.0]):
                demos = makeEquivalenceClassKey(variable.VarName, self.schema, self.VAR_COMPONENTS)
                hist[demos] += 1
        self.histogram = hist

    def buildHistogramFromDefaultDict(self, varsToValues:defaultdict) -> None:
        hist : defaultdict = defaultdict(lambda: 0)
        for varName in varsToValues.keys():
            #if 'dummy' not in varName and 'sum' not in varName and 'abs' not in varName and 'cdf' not in varName:
            if varName[:2] == "C_":
                #if varsToValues[varName] > 0:
                if numpy.isclose([varsToValues[varName]], [1.0]):
                    demos = makeEquivalenceClassKey(varName, self.schema, self.VAR_COMPONENTS)
                    hist[demos] += 1
        self.histogram = hist

@svCatch("Failed to make equivalence class key.")
def makeEquivalenceClassKey(varName:str, schema:List[str], var_components:List[str]) -> str:
    """
        Converts str var name in conventional format into equiv class string key, where equiv class is defined by
        matching on elements of split-list indicated by schema. Recall typical split-on-underscore
        var name looks like len=14 list:

     Placeholder  Block Geocode      Sex     StartAge  Person#   Sex     Age  White Black Aian Asian  NH    SO  Hisp
     ['C',       '483019501001743', 'male', '18',        '0',   'male', '19',  'Y',  'N',  'N', 'N',  'N', 'N',  'N']
    """
    varValues = {key:val for key, val in zip(var_components,varName.split('_'))}
    return '_'.join([varValues[key] for key in schema])

def getKeysForHistogram(variables:List[gurobipy.Var], initialValues:defaultdict, schema:List[str],
                        var_components:List[str]) -> Tuple[defaultdict,defaultdict]:
    """
        initialValues is a dict: str -> float containing initial values attained a prior solve
        of a possibly distinct base model (from the model containing variables). It is used to
        ensure that rhsSums is properly initialized, even when starting with a model
        (e.g., relaxedModel) that has not been optimized before, and so does not have var.X values.
    """
    rhsSums : defaultdict = defaultdict(lambda: 0)
    keyToVars : defaultdict = defaultdict(lambda: set([]))
    for v in variables:
        varName = v.VarName
        #if 'dummy' not in varName:
        if varName[:2] == "C_":
            key = makeEquivalenceClassKey(varName, schema, var_components)
            keyToVars[key].add(v)
            if varName in initialValues and numpy.isclose([initialValues[varName]], [1.0]):
                #logging.debug("getKeysForHist var ", varName, " w/ val ", initialValues[varName], " isclose ", numpy.isclose(initialValues[varName], 1.0))
                rhsSums[key] += 1
    return keyToVars, rhsSums
