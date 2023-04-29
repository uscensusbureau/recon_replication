--
-- Temporary table structure for view `geo_blocks`
--

DROP TABLE IF EXISTS `geo_blocks`;
/*!50001 DROP VIEW IF EXISTS `geo_blocks`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8MB4;
/*!50001 CREATE VIEW `geo_blocks` AS SELECT
 1 AS `geocode`,
 1 AS `state`,
 1 AS `county`,
 1 AS `tract`,
 1 AS `block`,
 1 AS `name`,
 1 AS `pop100`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `geo_counties`
--

DROP TABLE IF EXISTS `geo_counties`;
/*!50001 DROP VIEW IF EXISTS `geo_counties`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8MB4;
/*!50001 CREATE VIEW `geo_counties` AS SELECT
 1 AS `geocode`,
 1 AS `state`,
 1 AS `county`,
 1 AS `name`,
 1 AS `pop100`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `geo_tracts`
--

DROP TABLE IF EXISTS `geo_tracts`;
/*!50001 DROP VIEW IF EXISTS `geo_tracts`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8MB4;
/*!50001 CREATE VIEW `geo_tracts` AS SELECT
 1 AS `geocode`,
 1 AS `state`,
 1 AS `county`,
 1 AS `tract`,
 1 AS `name`,
 1 AS `pop100`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `tracts_geostatus`
--

DROP TABLE IF EXISTS `tracts_geostatus`;
/*!50001 DROP VIEW IF EXISTS `tracts_geostatus`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8MB4;
/*!50001 CREATE VIEW `tracts_geostatus` AS SELECT
 1 AS `geocode`,
 1 AS `state`,
 1 AS `county`,
 1 AS `tract`,
 1 AS `lp_end`,
 1 AS `sol_end`*/;
SET character_set_client = @saved_cs_client;

--
-- Final view structure for view `geo_blocks`
--

/*!50001 DROP VIEW IF EXISTS `geo_blocks`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50001 VIEW `geo_blocks` AS select concat(`geo`.`STATE`,`geo`.`COUNTY`,`geo`.`TRACT`,`geo`.`BLOCK`) AS `geocode`,`geo`.`STATE` AS `state`,`geo`.`COUNTY` AS `county`,`geo`.`TRACT` AS `tract`,`geo`.`BLOCK` AS `block`,`geo`.`NAME` AS `name`,`geo`.`POP100` AS `pop100` from `geo` where (`geo`.`SUMLEV` = '101') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `geo_counties`
--

/*!50001 DROP VIEW IF EXISTS `geo_counties`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50001 VIEW `geo_counties` AS select concat(`geo`.`STATE`,`geo`.`COUNTY`) AS `geocode`,`geo`.`STATE` AS `state`,`geo`.`COUNTY` AS `county`,`geo`.`NAME` AS `name`,`geo`.`POP100` AS `pop100` from `geo` where (`geo`.`SUMLEV` = '050') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `geo_tracts`
--

/*!50001 DROP VIEW IF EXISTS `geo_tracts`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50001 VIEW `geo_tracts` AS select concat(`geo`.`STATE`,`geo`.`COUNTY`,`geo`.`TRACT`) AS `geocode`,`geo`.`STATE` AS `state`,`geo`.`COUNTY` AS `county`,`geo`.`TRACT` AS `tract`,`geo`.`NAME` AS `name`,`geo`.`POP100` AS `pop100` from `geo` where (`geo`.`SUMLEV` = '140') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `tracts_geostatus`
--

/*!50001 DROP VIEW IF EXISTS `tracts_geostatus`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50001 VIEW `tracts_geostatus` AS select concat(`tracts`.`state`,`tracts`.`county`,`tracts`.`tract`) AS `geocode`,`tracts`.`state` AS `state`,`tracts`.`county` AS `county`,`tracts`.`tract` AS `tract`,`tracts`.`lp_end` AS `lp_end`,`tracts`.`sol_end` AS `sol_end` from `tracts` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-07-30 13:21:27
