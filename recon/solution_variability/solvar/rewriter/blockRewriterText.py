import typing
import pathlib
import gurobipy
import os
from solvar.core.utils import svMkDir
from solvar.core.filesystem import Filesystem, HDFS
from solvar.rewriter.lpModel import *
from solvar.rewriter.blockRewriter import ModelRewriter


class BlockLevelTextRewriter(ModelRewriter):
    @staticmethod
    def get_all_block_names(model: LPProgram) -> typing.Set[str]:
        """collect a set of all blocks referenced in a model"""
        block_names: typing.Set[str] = set()
        for constraint in model.getConstrs():
            block_name = ModelRewriter.get_block_name(constraint.name.name)
            if block_name is not None:
                block_names.add(block_name)
        return block_names

    def __init__(self, relax_constraint: bool):
        self.relax_constraint = relax_constraint

    def rewrite_text(self, file_contents: str) -> typing.Dict[str, str]:
        # load in the model struture as LPProgram
        lines = (file_contents.split('\n'))
        parser = LPModelParser()
        model: LPProgram = parser.parse(lines)

        # ds for function return
        rewritten_models: typing.Dict[str, str] = {}

        # get block names so that we can iterate
        block_names = self.get_all_block_names(model)

        # fill up array with new variables and constraints for the block models
        # NOTE: assume objective and goal don't change
        block_constrs = []
        block_vars = []
        for block_name in block_names:
            # get all block level constraints referencing the current blocks
            info = ([(constr, constr.lhs.expr) for constr in model.getConstrs() if self.get_block_name(constr.name.name) == block_name])
            constrs, variables = list(zip(*info))
            constrs = list(constrs)
            variables = set([i for v in variables for i in v])

            # account for tract level constraints if we are using them
            if self.relax_constraint:
                for constraint in model.getConstrs():
                    current_block_name = self.get_block_name(constraint.name.name)
                    if current_block_name is None:
                        lhs, sense, rhs, name = constraint.lhs, constraint.sense, constraint.rhs, constraint.name
                        lhs = LPLinearExpr([vi for vi in lhs.expr if vi.name.split('_')[1] == block_name])
                        final_size = len(lhs.expr)
                        if final_size > 0:
                            constrs.append(LPConstraint(name, lhs, "<=", rhs))
                            variables |= set(lhs.expr)

            # add the new constraints and variables (will write to a lp file later)
            block_constrs.append(constrs)
            block_vars.append(variables)

        # now, replace the modifications in the model and write the text to a file
        # note, we are mutating the model object, but this should be fine...
        for block_name, constrs, vars  in zip(block_names, block_constrs, block_vars):
            idx = [s.name == 'Subject To' for s in model.lines].index(True)
            model.lines[idx] = LPSection('Subject To', constrs)
            idx = [s.name == 'Binaries' for s in model.lines].index(True)
            model.lines[idx] = LPSection('Binaries', list(vars))
            #rewritten_models[f"block_{block_name}"] = model.as_text()
            yield f"block_{block_name}", model.as_text()
        #return rewritten_models

    def rewrite(self, model: gurobipy.Model) -> typing.Dict[str, gurobipy.Model]:
        """for the scope of the text rewriter, this is a bad interface (don't use gurobipy)"""
        raise NotImplementedError

    def _rewrite_lp_file(self, lp_path, output_dir, gurobi_env, fs):
        # TODO: this is weird
        local_path = lp_path
        if fs is not None:
            fs.downloadFile(lp_path, local_path)
            local_path = output_dir / lp_path.name
        with open(local_path, 'r') as fp:
            model_contents = fp.read()
        rmodels = self.rewrite_text(model_contents)
        res = {}
        for model_name, rmodel in rmodels:
            out_fname = pathlib.Path(output_dir) / f"{model_name}.lp"
            with open(out_fname, "w") as fp:
                fp.write(rmodel)
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
        resources = {}
        if not os.path.exists(output_dir):
            svMkDir(output_dir)
        #from joblib import Parallel, delayed
        #ret = Parallel(n_jobs=12)(delayed(self._rewrite_lp_file)(lp_path, output_dir, gurobi_env, fs) for lp_path in lp_files)
        ret = [self._rewrite_lp_file(lp_path, output_dir, gurobi_env, fs) for lp_path in lp_files]
        return dict(ret)


class BlockNoPctTextRewriter(BlockLevelTextRewriter):
    """configure block level rewriter to remove tract level constraints"""
    def __init__(self):
        super().__init__(relax_constraint=False)


class BlockIneqPctTextRewriter(BlockLevelTextRewriter):
    """configure block level rewriter to relax the tract level constraints"""
    def __init__(self):
        super().__init__(relax_constraint=True)
