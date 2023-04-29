"""
Class related to reading the configuration
"""

from solvar.core.utils import *
import configparser

@svCatch("Failed to load ini file.")
def svLoadIniFile(iniFile:Path) -> configparser.ConfigParser:
    xConfig = configparser.ConfigParser()
    xConfig.optionxform = str # type: ignore  ## Hack it to be case sensitive
    with open(iniFile, 'r') as f:
        xConfig.read_file(f)
    return xConfig

class Config:
    """
    The main configuration class.
    Data types and names of settings are stored as static class members.
    """
    section = 'solvar' #Currently we have one section, so no need to overcomplicate this class
    # choices for strs and ints
    choices = {
        'filesystem' : ['s3', 'hdfs']
    }

    def __init__(self, configFile:Path) -> None:
        self.configFile = configFile
        self.xConfig = svLoadIniFile(configFile)
        if (not self.xConfig.has_section(self.section)):
            svExit(f"Section <{self.section}> not found in <{configFile}>.")
        ### to see if some unwanted parameters are in config file
        self.optionsInConfig = dict.fromkeys(self.xConfig.options(self.section),False) 
        self.errorSuffix = f"section {self.section} in {self.configFile}"

    def warnAboutForeignOptions(self) -> None:
        #Warn about foreign options
        for option,isKnown in self.optionsInConfig.items():
            if (not isKnown):
                logging.warning(f"Unrecognized option <{option}> in section {self.section}."
                                f"This value is going to be ignored.")
            
    def checkSetting(self, xSetting:str) -> None:
        if (not self.xConfig.has_option(self.section,xSetting)):
            svExit(f"{xSetting} not found in {self.errorSuffix}.")
        self.optionsInConfig[xSetting] = True
        logging.debug(f"Config-CheckSetting: Reading {xSetting}.")

    def loadBool(self, boolSetting:str) -> bool:
        self.checkSetting(boolSetting)
        try:
            return self.xConfig.getboolean(self.section,boolSetting)
        except Exception as exc:
            svExit(f"Invalid boolean value for <{boolSetting}> in {self.errorSuffix}.")

    def loadInt(self, intSetting:str) -> int:
        self.checkSetting(intSetting)
        try:
            intVal = self.xConfig.getint(self.section,intSetting)
        except Exception as exc:
            svExit(f"Invalid integer value for <{intSetting}> in {self.errorSuffix}.")
        self.validateChoices(intSetting, intVal)
        return intVal

    def loadFloat(self, floatSetting:str) -> float:
        self.checkSetting(floatSetting)
        try:
            floatVal = self.xConfig.getfloat(self.section,floatSetting)
        except Exception as exc:
            svExit(f"Invalid floating point value for <{floatSetting}> in {self.errorSuffix}.")
        return floatVal

    def loadStr(self, strSetting:str) -> str:
        self.checkSetting(strSetting)
        try:
            strVal = self.xConfig.get(self.section,strSetting)
        except Exception as exc:
            svExit(f"Failed to read value of <{strSetting}> in {self.errorSuffix}.")
        self.validateChoices(strSetting, strVal)
        return strVal
        
    def validateChoices(self, xSetting:str, settingVal:Union[str,int]) -> None:
        if ((xSetting in self.choices) and (settingVal not in self.choices[xSetting])):
            svExit(f"Illegal value for <{xSetting}> in {self.errorSuffix}. Value has to be in {self.choices[xSetting]}.")


