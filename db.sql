use ENTER_DATABASE_NAME_HERE

CREATE TABLE `deletedIndex` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `deleteAt` date NOT NULL,
  `primaryPath` text NOT NULL,
  `secondaryPath` text NOT NULL,
  `location` text NOT NULL,
  `isDir` tinyint NOT NULL,
  `forceDelete` tinyint NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `primaryIndex` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `primaryPath` text NOT NULL,
  `secondaryPath` text NOT NULL,
  `modified` double NOT NULL,
  `size` bigint NOT NULL,
  `isDir` tinyint NOT NULL,
  `location` text NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
