const mysql = require('mysql2');
const { appendReport } = require('../utils/report.helper');
const { getDBDestination, getDBName,
  DDL: { TABLES, PROCEDURES, FUNCTIONS }
} = require('../configs/db');
const {
  saveToFile,
  makeSureFolderExisted,
  emptyDirectory,
  readFromFile
} = require('../utils/file.helper');

// Function to prepare the DDL folder
function makeDDLFolderReady(dbPath, DDL) {
  const ddLFolderPath = `${dbPath}/${DDL}`;
  // 1. get folder ready
  makeSureFolderExisted(ddLFolderPath);
  makeSureFolderExisted(`${dbPath}/current-ddl`);
  // 2. clean current ddl list
  emptyDirectory(ddLFolderPath);
  saveToFile(`${dbPath}/current-ddl`, `${DDL}.list`, '');
  // 3. make sure existed backup folder
  makeSureFolderExisted(`${dbPath}/backup/${DDL}`);
  return ddLFolderPath;
}

/**
 * Exports functions from a database connection.
 * 
 * @param {*} connection The database connection.
 * @param {*} dbConfig The database configuration.
 * @returns {Promise} A promise that resolves with the number of exported functions.
 */
async function exportFunctions(connection, dbConfig) {
  return new Promise((resolve, reject) => {
    const dbPath = `db/${dbConfig.envName}/${getDBName(dbConfig.envName)}`;
    const ddlFolderPath = makeDDLFolderReady(dbPath, FUNCTIONS);
    const functionQuery = 'SHOW FUNCTION STATUS WHERE Db = ?';
    connection.query(functionQuery, [dbConfig.database], async (err, functionResults) => {
      if (err) {
        alog.error('Error retrieving functions: ', err);
        connection.end();
        reject(err);
        return;
      }
      //
      await appendReport(dbConfig.envName, { 'functions_total': functionResults.length });
      // Export functions to separate files
      for (const row of functionResults) {
        const fnName = row.Name;
        const query = `SHOW CREATE FUNCTION ${fnName}`;
        connection.query(query, async (err, result) => {
          if (err) {
            alog.error(`Error retrieving create statement for ${folder}:: ${fnName}: `, err);
            reject(err);
            return;
          }
          //
          //
          const createStatement = result[0]['Create Function'];
          appendDDL(dbConfig.envName, ddlFolderPath, FUNCTIONS, fnName, convertKeywordsToUppercase(createStatement));
        });
      }
      return resolve(functionResults.length);
    });
  });
}
/**
 * exporter.js
 * 
 * This file contains a function to export procedures from a database connection.
 * 
 * @param {*} connection - The database connection.
 * @param {*} dbConfig - The database configuration.
 * @returns {Promise<number>} - A promise that resolves to the number of procedures exported.
 */
async function exportProcedures(connection, dbConfig) {
  return new Promise((resolve, reject) => {
    // Retrieve the database path and prepare the DDL folder
    const dbPath = `db/${dbConfig.envName}/${getDBName(dbConfig.envName)}`;
    const ddlFolderPath = makeDDLFolderReady(dbPath, PROCEDURES);

    // Query the database for procedure information
    const procedureQuery = 'SHOW PROCEDURE STATUS WHERE Db = ?';
    connection.query(procedureQuery, [dbConfig.database], async (err, procedureResults) => {
      if (err) {
        alog.error('Error retrieving procedures: ', err);
        connection.end();
        reject(err);
        return;
      }

      // Append the number of procedures to the report
      await appendReport(dbConfig.envName, { 'procedures_total': procedureResults.length });

      // Export procedures to separate files
      for (const row of procedureResults) {
        const spName = row.Name;
        const query = `SHOW CREATE PROCEDURE ${spName}`;
        connection.query(query, async (err, result) => {
          if (err) {
            alog.error(`Error retrieving create statement for ${folder}:: ${spName}: `, err);
            reject(err);
            return;
          }
          const createStatement = result[0]['Create Procedure'];
          appendDDL(dbConfig.envName, ddlFolderPath, PROCEDURES, spName, convertKeywordsToUppercase(createStatement));
        });
      }

      return resolve(procedureResults.length);
    });
  });
}

/**
 * 
 * @param {*} connection 
 * @returns 
 */
async function exportTables(connection, dbConfig) {
  return new Promise((resolve, reject) => {
    const dbPath = `db/${dbConfig.envName}/${getDBName(dbConfig.envName)}`;
    const ddlFolderPath = makeDDLFolderReady(dbPath, TABLES);
    const tableQuery = 'SHOW TABLES';
    connection.query(tableQuery, async (err, tableResults) => {
      if (err) {
        alog.error('Error retrieving tables: ', err);
        connection.end();
        reject(err);
        return;
      }
      //
      await appendReport(dbConfig.envName, { 'tables_total': tableResults.length });
      // Export tables to separate files
      for (const row of tableResults) {
        const tableName = Object.values(row)[0];
        const query = `SHOW CREATE TABLE \`${tableName}\``;
        connection.query(query, async (err, result) => {
          if (err) {
            alog.error(`Error retrieving create statement for ${folder}:: ${tableName}: `, err);
            reject(err);
            return;
          }
          //
          //
          const createStatement = result[0]['Create Table'];
          const rmvAIregex = /AUTO_INCREMENT=\d+\s/;
          appendDDL(dbConfig.envName, ddlFolderPath, TABLES, tableName, createStatement.replace(rmvAIregex, ''));
        });
      }
      return resolve(tableResults.length);
    });
  });
}

async function appendDDL(env, ddlFolderPath, ddlType, ddlName, createStatement) {
  const ddlFolder = `./db/${env}/${getDBName(env)}/current-ddl`;
  const ddlFile = `${ddlType}.list`;
  const allDll = readFromFile(ddlFolder, ddlFile, 1);
  const ddlList = [...allDll, ddlName];
  saveToFile(ddlFolder, ddlFile, `${ddlList.join('\n')}`);
  saveToFile(ddlFolderPath, `${ddlName}.sql`, createStatement);
}
/**
 * This function converts keywords in a query to uppercase.
 * It takes a query as input and returns the converted query with uppercase keywords.
 * Certain keywords are excluded from conversion, such as GROUP and USER, which are converted to lowercase.
 * The function also removes unnecessary characters and replaces tabs with spaces.
 */
function convertKeywordsToUppercase(query) {
  const keywords = [
    ...new Set(
      [
        'ACCESSIBLE', 'ADD', 'ALL', 'ALTER', 'ANALYZE', 'AND', 'AS', 'ASC', 'ASENSITIVE', 'BEFORE', 'BETWEEN', 'BIGINT', 'BINARY', 'BLOB', 'BOTH', 'BY',
        'CALL', 'CASCADE', 'CASE', 'CHANGE', 'CHAR', 'CHARACTER', 'CHECK', 'COLLATE', 'COLUMN', 'CONDITION', 'CONSTRAINT', 'CONTINUE', 'CONVERT', 'CREATE',
        'CROSS', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'CURSOR', 'DATABASE', 'DATABASES', 'DAY_HOUR', 'DAY_MICROSECOND', 'DAY_MINUTE',
        'DAY_SECOND', 'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DELAYED', 'DELETE', 'DESC', 'DESCRIBE', 'DETERMINISTIC', 'DISTINCT', 'DISTINCTROW', 'DIV',
        'DOUBLE', 'DROP', 'DUAL', 'EACH', 'ELSE', 'ELSEIF', 'ENCLOSED', 'ESCAPED', 'EXISTS', 'EXIT', 'EXPLAIN', 'FALSE', 'FETCH', 'FLOAT', 'FLOAT4', 'FLOAT8', 'FOR',
        'FORCE', 'FOREIGN', 'FROM', 'FULLTEXT', 'GENERATED', 'GET', 'GRANT', 'GROUP', 'HAVING', 'HIGH_PRIORITY', 'HOUR_MICROSECOND', 'HOUR_MINUTE', 'HOUR_SECOND',
        'IF', 'IGNORE', 'IGNORE_SERVER_IDS', 'IN', 'INDEX', 'INFILE', 'INNER', 'INOUT', 'INSENSITIVE', 'INSERT', 'INT', 'INT1', 'INT2', 'INT3', 'INT4', 'INT8', 'INTEGER',
        'INTERVAL', 'INTO', 'IO_AFTER_GTIDS', 'IO_BEFORE_GTIDS', 'IS', 'ITERATE', 'JOIN', 'KEY', 'KEYS', 'KILL', 'LEADING', 'LEAVE', 'LEFT', 'LIKE', 'LIMIT', 'LINEAR',
        'LINES', 'LOAD', 'LOCALTIME', 'LOCALTIMESTAMP', 'LOCK', 'LONG', 'LONGBLOB', 'LONGTEXT', 'LOOP', 'LOW_PRIORITY', 'MASTER_BIND', 'MASTER_SSL_VERIFY_SERVER_CERT',
        'MATCH', 'MAXVALUE', 'MEDIUMBLOB', 'MEDIUMINT', 'MEDIUMTEXT', 'MIDDLEINT', 'MINUTE_MICROSECOND', 'MINUTE_SECOND', 'MOD', 'MODIFIES', 'NATURAL', 'NOT',
        'NO_WRITE_TO_BINLOG', 'NULL', 'NUMERIC', 'ON', 'OPTIMIZE', 'OPTION', 'OPTIONALLY', 'OR', 'ORDER', 'OUT', 'OUTER', 'OUTFILE', 'PARTITION', 'PRECISION', 'PRIMARY',
        'PROCEDURE', 'PURGE', 'RANGE', 'READ', 'READS', 'READ_WRITE', 'REAL', 'REFERENCES', 'REGEXP', 'RELEASE', 'RENAME', 'REPEAT', 'REPLACE', 'REQUIRE', 'RESIGNAL',
        'RESTRICT', 'RETURN', 'REVOKE', 'RIGHT', 'RLIKE', 'SCHEMA', 'SCHEMAS', 'SECOND_MICROSECOND', 'SELECT', 'SENSITIVE', 'SEPARATOR', 'SET', 'SHOW', 'SIGNAL',
        'SMALLINT', 'SPATIAL', 'SPECIFIC', 'SQL', 'SQLEXCEPTION', 'SQLSTATE', 'SQLWARNING', 'SQL_BIG_RESULT', 'SQL_CALC_FOUND_ROWS', 'SQL_SMALL_RESULT', 'SSL',
        'STARTING', 'STORED', 'STRAIGHT_JOIN', 'TABLE', 'TERMINATED', 'TEXT', 'THEN', 'TINYBLOB', 'TINYINT', 'TINYTEXT', 'TO', 'TRAILING', 'TRIGGER', 'TRUE',
        'UNDO', 'UNION', 'UNIQUE', 'UNLOCK', 'UNSIGNED', 'UPDATE', 'USAGE', 'USE', 'USING', 'UTC_DATE', 'UTC_TIME', 'UTC_TIMESTAMP', 'VALUES', 'VARBINARY', 'VARCHAR',
        'VARCHARACTER', 'VARYING', 'VIRTUAL', 'WHEN', 'WHERE', 'WHILE', 'WITH', 'WRITE', 'XOR', 'YEAR_MONTH', 'ZEROFILL', 'END', 'OPEN', 'CLOSE', 'DUPLICATE', 'COALESCE'
      ]
    )
  ];
  // Split the query into individual words
  const words = query.split(/\b/);
  // Convert keywords to uppercase
  const convertedQuery = words
    .map(word => (keywords.includes(word.toUpperCase()) ? word.toUpperCase() : word))
    .join('')
    .replace(/\`(GROUP|USER)\`/g, (match, p1) => '`' + p1.toLowerCase() + '`')
    .replace(/\t/g, '  ')
    .replace(/\sDEFINER=`.+`@`(%|.+%)`\s/, ' ');
  return convertedQuery;
}
/**
 * Exporter function that exports the specified DDL (Data Definition Language) from a MySQL database.
 * 
 * @param {*} ddl - The type of DDL to export (e.g., TABLES, FUNCTIONS, PROCEDURES).
 * @returns {Function} - An async function that takes an environment object and exports the DDL.
 */
export const exporter = (ddl) => async (env) => {
  const start = Date.now();
  const dbConfig = getDBDestination(env);
  // Create a MySQL connection
  const connection = mysql.createConnection({
    host: dbConfig.host,
    database: dbConfig.database,
    user: dbConfig.user,
    password: dbConfig.password
  });
  // Connect to the MySQL server
  connection.connect(async (err) => {
    if (err) {
      alog.error('Error connecting to the database: ', err);
      return;
    }
    // Retrieve the list of DDL
    let rs;
    switch (ddl) {
      case TABLES:
        rs = await exportTables(connection, dbConfig);
        break;
      case FUNCTIONS:
        rs = await exportFunctions(connection, dbConfig);
        break;
      case PROCEDURES:
        rs = await exportProcedures(connection, dbConfig);
        break;
    }
    // Close the MySQL connection
    connection.end();
    alog.dev(`Exporting ${rs} ${env}.${getDBName(env)}.${ddl} done in:: ${Date.now() - start}ms`);
  });
};