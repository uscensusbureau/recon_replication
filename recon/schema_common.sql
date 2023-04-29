--
-- Table structure for table `errors`
--

DROP TABLE IF EXISTS `errors`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8MB4 */;
CREATE TABLE `errors` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `t` datetime DEFAULT CURRENT_TIMESTAMP,
  `host` varchar(256) DEFAULT NULL,
  `error` mediumtext,
  `argv0` varchar(128) DEFAULT NULL,
  `reident` varchar(256) DEFAULT NULL,
  `file` varchar(64) DEFAULT NULL,
  `line` int(11) DEFAULT NULL,
  `stack` mediumtext,
  `last_value` mediumtext,
  `stusab` varchar(2) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `county` varchar(3) DEFAULT NULL,
  `tract` varchar(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `t` (`t`),
  KEY `host` (`host`(255)),
  KEY `reident` (`reident`(255)),
  KEY `file` (`file`),
  KEY `line` (`line`)
) ENGINE=InnoDB AUTO_INCREMENT=481 DEFAULT CHARSET=utf8MB4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `sysload`
--

DROP TABLE IF EXISTS `sysload`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8MB4 */;
CREATE TABLE `sysload` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `t` datetime NOT NULL,
  `host` varchar(64) NOT NULL,
  `min1` decimal(6,2) DEFAULT NULL,
  `min5` decimal(6,2) DEFAULT NULL,
  `min15` decimal(6,2) DEFAULT NULL,
  `freegb` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `t` (`t`,`host`),
  KEY `host` (`host`),
  KEY `min1` (`min1`),
  KEY `min5` (`min5`),
  KEY `min15` (`min15`),
  KEY `freegb` (`freegb`)
) ENGINE=InnoDB AUTO_INCREMENT=490435 DEFAULT CHARSET=utf8MB4;
/*!40101 SET character_set_client = @saved_cs_client */;
