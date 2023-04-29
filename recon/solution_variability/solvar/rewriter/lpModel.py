"""
Handmade LP Model Parser (to bypass needing gurobi for this)
"""
import re
import typing
import abc


## Language Elements

class LPBase(abc.ABC):
    """base element for parsetree (parsetree will only contain objects who are based on LpBase)"""
    @abc.abstractmethod
    def as_text(self) -> str:
        return ''

    def __repr__(self):
        t = self.as_text()
        t = t if len(t) < 80 else f"{t[:80]}..."
        return f"<{self.__class__.__name__} <text: {t}>>"


class LPLine(LPBase):
    """LP program line (used to capture elements that we don't care to parse)"""
    def __init__(self, line):
        self.line = line

    def as_text(self) -> str:
        return self.line


class LPComment(LPBase):
    """LP program comment (e.g. /* comment */)"""
    def __init__(self, comment):
        self.comment = comment

    def as_text(self) -> str:
        return f"\\* {self.comment} *\\"


class LPIdentifier(LPBase):
    """"LP program identifier"""
    def __init__(self, name: str):
        self.name = name

    def as_text(self) -> str:
        return self.name

    # this really should not be implemented for just Identifier -- maybe
    # put this in the base object, and handle the hashable stuff in a data structure
    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        """this is needed so that identifiers can be used in sets and dicts, keeping unique names"""
        return hash(self.name)


class LPFloat(LPBase):
    """LP program float"""
    def __init__(self, float):
        self.float = float

    def as_text(self) -> str:
        return self.float


class LPLinearExpr(LPBase):
    """LP program linear expression"""
    def __init__(self, expr: typing.Sequence[typing.Union[LPIdentifier, LPFloat]]):
        self.expr = expr

    def as_text(self) -> str:
        return " + ".join([e.as_text() for e in self.expr])


class LPConstraint(LPBase):
    """LP program constraint"""
    def __init__(self, name: LPIdentifier, lhs: LPLinearExpr, sense: str, rhs: LPLinearExpr):
        self.name = name
        self.lhs = lhs
        self.rhs = rhs
        self.sense = sense

    def as_text(self) -> str:
        return f"{self.name.as_text()}: {self.lhs.as_text()} {self.sense} {self.rhs.as_text()}"


class LPSection(LPBase):
    """LP program section"""
    def __init__(self, name, lines):
        self.name = name
        self.lines = lines

    def as_text(self) -> str:
        if isinstance(self.name, str):
            return f"{self.name}" + '\n' + '\n'.join([line.as_text() for line in self.lines])
        else:
            return '\n'.join([line.as_text() for line in self.lines])


class LPProgram(LPBase):
    """LP program"""
    def __init__(self, lines):
        self.lines = lines

    def getConstrs(self):
        s = [s for s in self.lines if isinstance(s, LPSection) and s.name == "Subject To"]
        if len(s) == 1:
            section = s[0]
            for constr in section.lines:
                yield constr
        else:
            raise RuntimeError

    def as_text(self) -> str:
        return '\n'.join([line.as_text() for line in self.lines]) + '\nEND'


## Parser


class LPModelParser:
    """
    LP Program Parser

    This parses to an incomplete representation of an LP program. We do not have

    * a complete representation for the objective and bounds
    * the ability to parse multi-line linear expressions

    LP programs are parsed into sections {head, subject to, binaries, bounds} and then each section is subsequently
    parsed.

    * head contains the objective and other things that we skip (unused)
    * subject to is parsed into a list of constraints
    * binaries is parsed into a list of variables (via identifiers)
    """
    def __init__(self):
        # define regex patterns
        self.match_identifier = re.compile(r"[a-zA-Z0-9_\.]+")
        self.match_constraint = re.compile(rf"\s*([a-zA-Z0-9_]+)\s*:\s*(.*)\s*([=<>]+)\s*(.*)\s*")
        self.match_comment = re.compile(r"\\\*(.*)\*\\")
        self.match_identifier_line = re.compile(rf"\s*([a-zA-Z0-9_\.]+)\s*")
        self.sections = [
            re.compile("\s*(Subject To)\s*"),
            re.compile("\s*(Bounds)\s*"),
            re.compile("\s*(Binaries)\s*"),
            re.compile("\s*(END)\s*")
        ]

        # define permissible patterns in the sections
        self.re_matches = {
            None : [(self.match_comment, self.parse_comment)],
            'Subject To' : [(self.match_constraint, self.parse_constraint),
                            (self.match_comment, self.parse_comment)],
            'Binaries' : [(self.match_identifier_line, self.parse_identifier),
                          (self.match_comment, self.parse_comment)],
            'Bounds': [],
        }

    def split_sections(self, lines):
        """given a LP program with lines, split the lines into their identified sections (self.sections)"""
        sections = []
        sec = []
        sec_name = None
        for line in lines:
            matches = [re.match(pattern, line) for pattern in self.sections]
            if any([m is not None for m in matches]):
                #sections.append((sec_name, sec))
                yield (sec_name, sec)
                idx = [m is not None for m in matches].index(True)
                match = matches[idx]
                sec_name = match.group(1)
                sec = []
            else:
                sec.append(line)
        if len(sec) > 0:
             yield  (sec_name, sec)

    def parse(self, lines):
        """main parsing method"""
        program = []
        sections_split = self.split_sections(lines)

        # parse each section
        for section_name, lines in sections_split:
            section_matches = self.re_matches[section_name]
            sections_lines = []

            # parse by line
            # note that this approach means that we cannot do multiline stuff, but that should be possible to add if
            # need be
            for line in lines:
                matches_is_not_none = [re.match(mre, line) is not None for mre, _ in section_matches]

                # if match, exercise the correct parsing method, else treat it as a catch all line
                if any(matches_is_not_none):
                    n_matches = matches_is_not_none.count(True)
                    # ensure that only one match occured and then call the right method
                    if n_matches == 1:
                        idx = matches_is_not_none.index(True)
                        grp = re.match(section_matches[idx][0], line)
                        sections_lines.append(section_matches[idx][1](grp))
                    else:
                        raise RuntimeError(f"More than one match found for {n_matches} (section {section_name})")
                else:
                    sections_lines.append(LPLine(line))
            lp_section = LPSection(section_name, sections_lines)
            program.append(lp_section)

        p = LPProgram(program)
        return p

    def parse_constraint(self, groups):
        """given a regex match, convert to the right LPBase objects"""
        def get_expr(s):
            terms = [si.strip() for si in re.split(r"\s*[-+]\s*", s)]
            terms = [(LPIdentifier(si) if re.match(self.match_identifier, si) else float(si)) for si in terms]
            return LPLinearExpr(terms)
        name = LPIdentifier(groups.group(1))
        lhs = get_expr(groups.group(2))
        sense = groups.group(3)
        rhs = get_expr(groups.group(4))
        constr = LPConstraint(name, lhs, sense, rhs)
        return constr

    def parse_comment(self, groups):
        return LPComment(groups.group(1))

    def parse_identifier(self, groups):
        return LPIdentifier(groups.group(1))
