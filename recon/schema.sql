-- MySQL dump 10.13  Distrib 5.6.44, for Linux (x86_64)
--
-- Host: mysql.simson.net    Database: recon
-- ------------------------------------------------------
-- Server version	5.6.34-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8MB4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `addme`
--


--
-- Table structure for table `geo`
--

DROP TABLE IF EXISTS `geo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8MB4 */;
CREATE TABLE `geo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `STUSAB` char(2) NOT NULL,
  `SUMLEV` varchar(3) NOT NULL,
  `LOGRECNO` varchar(7) NOT NULL,
  `STATE` varchar(2) NOT NULL,
  `COUNTY` varchar(3) NOT NULL,
  `TRACT` varchar(6) DEFAULT NULL,
  `BLOCK` varchar(4) DEFAULT NULL,
  `NAME` varchar(90) DEFAULT NULL,
  `POP100` int(9) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `STUSAB` (`STUSAB`,`COUNTY`,`TRACT`,`BLOCK`),
  KEY `STATE` (`STATE`),
  KEY `LOGRECNO` (`LOGRECNO`),
  KEY `COUNTY` (`COUNTY`),
  KEY `TRACT` (`TRACT`),
  KEY `BLOCK` (`BLOCK`),
  KEY `NAME` (`NAME`),
  KEY `POP100` (`POP100`),
  KEY `SUMLEV` (`SUMLEV`),
  KEY `STATE_2` (`STATE`,`COUNTY`,`TRACT`,`BLOCK`)
) ENGINE=InnoDB AUTO_INCREMENT=13151296 DEFAULT CHARSET=utf8MB4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glog`
--

DROP TABLE IF EXISTS `glog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8MB4 */;
CREATE TABLE `glog` (
  `gurobi_version` varchar(254) DEFAULT NULL,
  `rows` int(8) DEFAULT NULL,
  `columns` int(8) DEFAULT NULL,
  `nonzeros` int(8) DEFAULT NULL,
  `presolve_rows` int(8) DEFAULT NULL,
  `presolve_NZ` int(8) DEFAULT NULL,
  `integer_vars` int(8) DEFAULT NULL,
  `binary_vars` int(8) DEFAULT NULL,
  `simplex_iterations` int(8) DEFAULT NULL,
  `seconds` float DEFAULT NULL,
  `start` varchar(254) DEFAULT NULL,
  `state` varchar(254) DEFAULT NULL,
  `county` varchar(254) DEFAULT NULL,
  `tract` varchar(254) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8MB4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tracts`
--

DROP TABLE IF EXISTS `tracts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8MB4 */;
CREATE TABLE `tracts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stusab` varchar(2) NOT NULL,
  `state` char(2) NOT NULL,
  `county` char(3) NOT NULL,
  `tract` char(6) NOT NULL,
  `lp_start` datetime DEFAULT NULL,
  `lp_end` datetime DEFAULT NULL,
  `lp_gb` int(6) DEFAULT NULL,
  `lp_host` varchar(64) DEFAULT NULL,
  `lp_validated` datetime DEFAULT NULL,
  `sol_start` datetime DEFAULT NULL,
  `sol_end` datetime DEFAULT NULL,
  `sol_gb` int(6) DEFAULT NULL,
  `sol_host` varchar(64) DEFAULT NULL,
  `sol_time` double DEFAULT NULL,
  `sol_validated` datetime DEFAULT NULL,
  `pop100`    int(11)  DEFAULT NULL,
  `final_pop` int(11) DEFAULT NULL,
  `NumVars` int(11) DEFAULT NULL,
  `NumConstrs` int(11) DEFAULT NULL,
  `NumNZs` int(11) DEFAULT NULL,
  `NumIntVars` int(11) DEFAULT NULL,
  `MIPGap` float DEFAULT NULL,
  `Runtime` float DEFAULT NULL,
  `Nodes` int(11) DEFAULT NULL,
  `IterCount` float DEFAULT NULL,
  `BarIterCount` float DEFAULT NULL,
  `isMIP` int(11) DEFAULT NULL,
  `hostlock` varchar(64) DEFAULT NULL,
  `pid` int(11) DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `csv_start` datetime DEFAULT NULL,
  `csv_end` datetime DEFAULT NULL,
  `csv_host` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `state` (`stusab`,`county`,`tract`),
  UNIQUE KEY `state_2` (`state`,`county`,`tract`),
  KEY `lp_start` (`lp_start`),
  KEY `lp_end` (`lp_end`),
  KEY `lp_validated` (`lp_validated`),
  KEY `sol_start` (`sol_start`),
  KEY `sol_end` (`sol_end`),
  KEY `sol_validated` (`sol_validated`),
  KEY `pop100`  (`pop100`),
  KEY `final_pop` (`final_pop`),
  KEY `hostlock` (`hostlock`),
  KEY `PID` (`pid`),
  KEY `csv_start` (`csv_start`),
  KEY `csv_end` (`csv_end`)
) ENGINE=InnoDB AUTO_INCREMENT=74081 DEFAULT CHARSET=utf8MB4;
/*!40101 SET character_set_client = @saved_cs_client */;
