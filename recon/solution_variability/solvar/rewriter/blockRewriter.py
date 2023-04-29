"""
LP Model Rewriter
"""
import typing
import pathlib
import gurobipy
import os
from solvar.core.utils import svMkDir
from solvar.core.filesystem import Filesystem, HDFS
from solvar.rewriter.lpModel import *


class ModelRewriter(abc.ABC):
    @staticmethod
    def get_block_name(constraint_name: str) -> typing.Optional[str]:
        """given a constraint, return the block name referenced or return none"""
        chunked = constraint_name.split("_")
        if chunked[2].startswith("PCT"):
            return None
        return chunked[3]

    @staticmethod
    def compute_population(model: gurobipy.Model):
        """compute population by counting RHSs of block level constraints"""
        with gurobipy.Env() as env:
            env.setParam('OutputFlag', 0)
            env.start()
            count = 0
            for constraint in (model.getConstrs()):
                if "_C_P0010001" in constraint.ConstrName:
                    count += constraint.RHS
        return count

    @staticmethod
    def get_all_block_names(model: gurobipy.Model) -> typing.Set[str]:
        """collect a set of all blocks referenced in a model"""
        block_names: typing.Set[str] = set()
        for constraint in model.getConstrs():
            block_name = ModelRewriter.get_block_name(constraint.ConstrName)
            if block_name is not None:
                block_names.add(block_name)
        return block_names

    def _rewrite_lp_file(self, lp_path, output_dir, gurobi_env, fs):
        # TODO: this is weird
        local_path = lp_path
        if fs is not None:
            fs.downloadFile(lp_path, local_path)
            local_path = output_dir / lp_path.name
        model = gurobipy.read(str(local_path), gurobi_env)
        rmodels = self.rewrite(model)
        res = {}
        for model_name, rmodel in rmodels:
            out_fname = pathlib.Path(output_dir) / f"{model_name}.lp"
            rmodel.write(str(out_fname))
            upload_path = lp_path.parent / out_fname.name
            if fs is not None:
                fs.uploadFile(out_fname, upload_path)
            res[model_name] = upload_path
        return (lp_path, res)

    def rewrite_lp_files(self,
                         fs: typing.Optional[Filesystem],
                         output_dir: pathlib.Path,
                         lp_files: typing.Sequence[pathlib.Path],
                         gurobi_env: typing.Optional[gurobipy.Env] = None) -> typing.Dict[pathlib.Path, typing.Dict[str, pathlib.Path]]:
        """take input .lp filepaths, rewrite the models decsribed in them, and write the rewritten models to output_dir"""
        resources = {}
        if not os.path.exists(output_dir):
            svMkDir(output_dir)
        from joblib import Parallel, delayed
        ret = Parallel(n_jobs=12)(delayed(self._rewrite_lp_file)(lp_path, output_dir, gurobi_env, fs) for lp_path in lp_files)
        return dict(ret)
        """
        for lp_path in lp_files:
            # TODO: this is weird
            local_path = lp_path
            if fs is not None:
                fs.downloadFile(lp_path, local_path)
                local_path = output_dir / lp_path.name
            model = gurobipy.read(str(local_path), gurobi_env)
            rmodels = self.rewrite(model)
            res = {}
            for model_name, rmodel in rmodels.items():
                out_fname = pathlib.Path(output_dir) / f"{model_name}.lp"
                rmodel.write(str(out_fname))
                upload_path = lp_path.parent / out_fname.name
                if fs is not None:
                    fs.uploadFile(out_fname, upload_path)
                res[model_name] = upload_path
            resources[lp_path] = res
        return resources
        """

    @abc.abstractmethod
    def rewrite(self, model: gurobipy.Model) -> typing.Dict[str, gurobipy.Model]:
        """rewrite the gurobipy model"""
        raise NotImplementedError


class TractLevelRewriter(ModelRewriter):
    """"""
    def rewrite(self, model: gurobipy.Model) -> typing.Dict[str, gurobipy.Model]:
        # TODO: get the correct name for this
        return {'tract' : model.copy()}


class BlockLevelRewriter(ModelRewriter):
    """implement the block level rewriting (both variations)"""
    def __init__(self, relax_constraint: bool):
        self.relax_constraint = relax_constraint

    def rewrite(self, model: gurobipy.Model) -> typing.Dict[str, gurobipy.Model]:
        """implement the block level rewrite changes

        A tract-level model containing $N$ blocks is transformed into $N$ block level models. There are three categories of
        models---tract, no-pct, pct-ineq---that have the following modifications:

        ### Tract Level Model (`tract`)
        **Modifications:**

        None

        ### Block-Level Model with No Tract Level Constraints (`no-pct`)

        **Modifications:**


        For every tract-level constraint (i.e. one containing `PCT` in the identifier), remove it from the MILP model.

        ### Block-Level Model with Modified Tract Level Constraints (`pct-ineq`)

        **Modifications:**


        For every tract-level constraint (i.e. one containing `PCT` in the identifier),

        * Remove all terms in the LHS linear expression that don't refer to the current block
        * Change the sense from `==` to `<=`
        """
        def generate_var(lhs):
            if isinstance(lhs, float):
                return
            for i in range(lhs.size()):
                yield lhs.getVar(i)

        rewritten_models: typing.Dict[str, gurobipy.Model] = {}
        block_names = self.get_all_block_names(model)

        for block_name in block_names:
            block_model = gurobipy.Model()
            model_vars = {}

            def submit_variable(bm, v: gurobipy.Var):
                if v.VarName in model_vars:
                    return model_vars[v.VarName]
                else:
                    vn = bm.addVar(ub=v.UB, lb=v.LB, obj=v.Obj, vtype=v.VType, name=v.VarName)
                    model_vars[v.VarName] = vn
                    return vn

            # this is where you could add a progress bar...
            for constraint in model.getConstrs():
                current_block_name = self.get_block_name(constraint.ConstrName)
                if current_block_name:
                    if current_block_name == block_name:
                        # keep this constraint as is
                        lhs, sense, rhs, name = model.getRow(constraint), constraint.Sense, constraint.RHS, constraint.ConstrName
                        terms = (submit_variable(block_model, vi) for vi in generate_var(lhs))
                        block_model.addConstr(sum(terms), sense, rhs, name)
                    else:
                        continue
                else:
                    lhs, sense, rhs, name = model.getRow(constraint), constraint.Sense, constraint.RHS, constraint.ConstrName
                    lhs = gurobipy.LinExpr(sum(vi for vi in generate_var(lhs) if vi.VarName.split('_')[1] == block_name))
                    terms = (submit_variable(block_model, vi) for vi in generate_var(lhs))
                    final_size = lhs.size()
                    if self.relax_constraint:
                        if final_size > 0:
                            block_model.addConstr(sum(terms), "<=", rhs, name)
            block_model.update()
            yield f"block_{block_name}", block_model
            #rewritten_models[f"block_{block_name}"] = block_model
        #return rewritten_models


class BlockNoPctRewriter(BlockLevelRewriter):
    """configure block level rewriter to remove tract level constraints"""
    def __init__(self):
        super().__init__(relax_constraint=False)


class BlockIneqPctRewriter(BlockLevelRewriter):
    """configure block level rewriter to relax the tract level constraints"""
    def __init__(self):
        super().__init__(relax_constraint=True)
