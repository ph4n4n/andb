const path = require('path');
const {
  saveToFile,
  makeSureFolderExisted,
  emptyDirectory,
  readFromFile
} = require('../utils/file.helper');
const { getSourceEnv, getDestEnv, getDBName,
  DDL: {
    FUNCTIONS,
    PROCEDURES,
    TABLES
  }
} = require('../configs/db');
const { appendReport, report2html, report2console
  , vimDiffToHtml } = require('../utils/report.helper');
const { log } = require('console');
/**
 * 
 * @param {*} tableSQL 
 * @returns 
 */
function parseTableDefinition(tableSQL) {
  try {
    const lines = tableSQL.split('\n');
    const tableNameLine = lines.find(line => line.includes('CREATE TABLE'));
    const tableNameMatch = tableNameLine.match(/`([^`]+)`/);

    if (!tableNameMatch || tableNameMatch.length < 2) {
      alog.error('Error parsing table name');
      return null;
    }
    const tableName = tableNameMatch[1];
    const columnDefs = [];
    const primaryKey = [];
    const indexes = {};
    let insideColumnDefinitions = false;
    let insideIndexDefinitions = false;

    for (const line of lines) {
      if (line.includes('CREATE TABLE')) {
        insideColumnDefinitions = true;
        continue;
      } else if (insideColumnDefinitions && line.trim().includes('ENGINE=InnoDB DEFAULT')) {
        // Reached the end of column definitions
        insideColumnDefinitions = false;
      } else if (line.includes('PRIMARY KEY') || line.includes('UNIQUE KEY') || line.includes('KEY')) {
        insideIndexDefinitions = true;
        const indexNameMatch = line.match(/`([^`]+)`/);
        if (indexNameMatch && indexNameMatch.length >= 2) {
          const indexName = indexNameMatch[1];
          if (line.includes('PRIMARY KEY')) {
            primaryKey.push(indexName);
          } else {
            indexes[indexName] = line.trim();
          }
        }
      } else if (insideColumnDefinitions && line.trim() !== '') {
        // Parse only non-empty lines inside column definitions
        const columnNameMatch = line.match(/`([^`]+)`/);
        if (!columnNameMatch || columnNameMatch.length < 2) {
          alog.error('Error parsing column name');
          return null;
        }
        const columnName = columnNameMatch[1];
        columnDefs.push({
          name: columnName,
          definition: line.trim(),
        });
      } else if (insideIndexDefinitions && line.trim() === ')') {
        insideIndexDefinitions = false;
      }
    }

    const columns = {};
    for (const columnDef of columnDefs) {
      columns[columnDef.name] = columnDef.definition;
    }

    return {
      tableName,
      columns,
      primaryKey,
      indexes,
    };
  } catch (error) {
    alog.error('Error parsing table definition:', error);
    return null;
  }
}
/**
 * 
 * @param {*} srcTableDefinition 
 * @param {*} destTableDefinition 
 * @returns 
 */
function generateAlterSQL(srcTableDefinition, destTableDefinition) {
  // 1. Compare primary keys NOT allowed
  // if (srcTableDefinition.primaryKey.join(',') !== destTableDefinition.primaryKey.join(',')) {
  //    alterSQL.push(`DROP PRIMARY KEY, ADD PRIMARY KEY (${srcTableDefinition.primaryKey.join(',')})`);
  // }
  // 2. Compare columns
  const { alterColumns, missingColumns } = compareColumns(srcTableDefinition, destTableDefinition);
  // 3. Compare Indexes
  const alterIndexes = compareIndexes(srcTableDefinition, destTableDefinition);
  // 4. write it down
  const tableName = srcTableDefinition.tableName;
  return {
    columns: !alterColumns?.length ? null
      : generateAlter(tableName, alterColumns),
    indexes: !alterIndexes?.length
      ? null
      : generateAlter(tableName, alterIndexes),
    missingColumns
  };
}
/**
 * 
 * @param {*} tableName 
 * @param {*} alters 
 * @returns 
 */
function generateAlter(tableName, alters) {
  return `ALTER TABLE ${tableName}\n${alters.join(',\n')};`
    .replace(/,,/g, ',')
    .replace(/,;/g, ';');
}
/**
 * 
 * @param {*} srcTableDefinition 
 * @param {*} destTableDefinition 
 * @returns 
 */
function compareColumns(srcTableDefinition, destTableDefinition) {
  const alterColumns = [];
  const missingColumns = [];

  let prevColumnName = null;
  // Check if any columns are missing in the destination table
  for (const columnName in srcTableDefinition.columns) {
    if (!destTableDefinition.columns[columnName]) {
      alterColumns.push(`ADD COLUMN ${srcTableDefinition.columns[columnName].replace(/[,;]$/, '')} AFTER \`${prevColumnName || 'FIRST'}\``);
    }
    prevColumnName = columnName;
  }
  // Reset previous column for the next loop
  prevColumnName = null;

  // Check if any columns have different definitions or have been renamed
  for (const srcColName in srcTableDefinition.columns) {
    if (destTableDefinition.columns[srcColName]) {
      const srcColumnDef = srcTableDefinition.columns[srcColName];
      const destColumnDef = destTableDefinition.columns[srcColName];

      if (srcColumnDef !== destColumnDef) {
        console.log(srcColumnDef, destColumnDef);
        // Column definition has changed
        alterColumns.push(`CHANGE COLUMN ${srcColName} ${srcColumnDef}`);
      }
    }
  }
  // Check if any columns are missing in the source table
  for (const destColName in destTableDefinition.columns) {
    if (!srcTableDefinition.columns[destColName]) {
      missingColumns.push(destColName);
    }
  }
  return { alterColumns, missingColumns };
}
/**
 * 
 * @param {*} srcTableDefinition 
 * @param {*} destTableDefinition 
 * @returns 
 */
function compareIndexes(srcTableDefinition, destTableDefinition) {
  const alterIndexes = [];
  // Compare other indexes
  const srcOtherIndexes = Object.keys(srcTableDefinition.indexes);
  const destOtherIndexes = Object.keys(destTableDefinition.indexes);

  for (const indexName of srcOtherIndexes) {
    if (!destOtherIndexes.includes(indexName)) {
      alterIndexes.push(`ADD ${srcTableDefinition.indexes[indexName]}`);
    }
  }
  const removeBTREE = (idx) => idx.replace(/\s?(,|USING BTREE)/g, '').trim();

  for (const indexName of destOtherIndexes) {
    if (!srcOtherIndexes.includes(indexName)) {
      // alterIndexes.push(`DROP INDEX ${indexName}`);
    } else if (removeBTREE(srcTableDefinition.indexes[indexName])
      !== removeBTREE(destTableDefinition.indexes[indexName])) {
      alterIndexes.push(`DROP INDEX ${indexName},
      ADD ${srcTableDefinition.indexes[indexName]}`);
    }
  }
  return alterIndexes;
}
/**
 * 
 * @param {*} env 
 * @param {*} tables 
 * @returns 
 */
async function reportTableStructureChange(env, tables = []) {
  const mapFolder = path.join(`map-migrate`, `${getSourceEnv(env)}-to-${env}`);
  const reportFolder = `${mapFolder}/${getDBName(getSourceEnv(env))}`;
  makeSureFolderExisted(reportFolder);
  //
  if (!tables.length) {
    // reset
    emptyDirectory(path.join(`${reportFolder}/tables/alters/columns`));
    emptyDirectory(path.join(`${reportFolder}/tables/alters/indexes`));
    saveToFile(`${reportFolder}/tables`, `alter-columns.list`, '');
    saveToFile(`${reportFolder}/tables`, `alter-indexes.list`, '');
    updateReport(env, 'columns', reportFolder, [], 'columns_updated');
    updateReport(env, 'indexes', reportFolder, [], 'indexes_updated');

    const tableFolder = `db/${env}/${getDBName(env)}/current-ddl`;
    const tables = readFromFile(tableFolder, `${TABLES}.list`, true)?.filter(Boolean);
    appendReport(env, { columns_missing: null });
    // Ahihi: no table found!
    if (!tables.length) {
      return false;
    }
    return reportTableStructureChange(env, tables);
  }
  // main compare
  const tblNeedAlterColumns = [];
  const tblNeedAlterIndexes = [];
  for (const tableName of tables) {
    try {
      // 1. Read table from .sql file
      const {
        srcTableDefinition = null,
        destTableDefinition = null,
        msg = ''
      } = readSrcDestFromFiles(tableName, env);
      // 
      if (!srcTableDefinition || !destTableDefinition) {
        alog.error(msg || `Error parsing table definitions for "${tableName}".`);
        continue;
      }
      // 2. generate alter table.columns
      const alters = generateAlterSQL(srcTableDefinition, destTableDefinition);
      //
      if (alters?.columns) {
        writeAlter(env, tableName, 'columns', alters?.columns);
        tblNeedAlterColumns.push(tableName);
      }
      //
      if (alters?.indexes) {
        writeAlter(env, tableName, 'indexes', alters?.indexes);
        tblNeedAlterIndexes.push(tableName);
      }
      // missing columns
      if (alters?.missingColumns.length) {
        appendReport(env, { columns_missing: { [tableName]: alters.missingColumns } });
      }
    } catch (error) {
      alog.error(`Error comparing table ${tableName}:`, error);
    }
  }
  // record alters
  makeSureFolderExisted(`${reportFolder}/tables`);
  //
  if (tblNeedAlterColumns.length) {
    updateReport(env, 'columns', reportFolder, tblNeedAlterColumns, 'columns_updated');
  }
  //
  if (tblNeedAlterIndexes.length) {
    updateReport(env, 'indexes', reportFolder, tblNeedAlterIndexes, 'indexes_updated');
  }
  //
  appendReport(env, { tables_updated: tblNeedAlterColumns.length + tblNeedAlterIndexes.length });
}
/**
 * 
 * @param {*} env 
 * @param {*} reportFolder 
 * @param {*} tblNeedAlters 
 * @param {*} keyChanges 
 */
function updateReport(env, alterType, reportFolder, tblNeedAlters, keyChanges) {
  saveToFile(`${reportFolder}/tables`, `alter-${alterType}.list`, [
    ...new Set(tblNeedAlters.filter(Boolean))
  ].join('\n'));
  appendReport(env, { [keyChanges]: tblNeedAlters.length });
}
/**
 * 
 * @param {*} tableName 
 * @param {*} type 
 * @param {*} alters 
 */
function writeAlter(env, tableName, type, alters) {
  const fTable = path.join(`map-migrate`, `${getSourceEnv(env)}-to-${env}`, `${getDBName(getSourceEnv(env))}`, `tables`);
  makeSureFolderExisted(`${fTable}/alters/${type}`);
  saveToFile(`${fTable}/alters/${type}`, `${tableName}.sql`, alters);
}
/**
 * 
 * @param {*} tableName 
 * @param {*} env 
 * @returns 
 */
function readSrcDestFromFiles(tableName, env) {
  const srcFolder = `db/${getSourceEnv(env)}/${getDBName(getSourceEnv(env))}/tables`;
  const destFolder = `db/${env}/${getDBName(env)}/tables`
  const srcFContent = readFromFile(srcFolder, `${tableName}.sql`);
  const destFContent = readFromFile(destFolder, `${tableName}.sql`);

  if (!srcFContent) {
    return { msg: `Table: ${getSourceEnv(env)}:"${tableName}" not existed!` };
  }
  if (!destFContent) {
    return { msg: `Table: ${env}:"${tableName}" not existed!` };
  }
  return {
    srcTableDefinition: parseTableDefinition(srcFContent),
    destTableDefinition: parseTableDefinition(destFContent)
  }
}
/**
 * 
 * @param {*} reportDDLFolder 
 * @param {*} srcLines 
 * @param {*} destLines 
 * @param {*} ddlType 
 * @param {*} destEnv 
 * @returns 
 */
async function markNewDDL(reportDDLFolder, srcLines, destLines, ddlType, destEnv) {
  const newDDL = `new.list`;
  makeSureFolderExisted(reportDDLFolder);
  saveToFile(reportDDLFolder, newDDL, '');

  const newLines = srcLines.filter(line => !destLines.includes(line)).filter(Boolean);
  if (newLines.length > 0) {
    const result = newLines.sort().join('\n');
    saveToFile(reportDDLFolder, newDDL, result);
  }
  // add to report
  return { [`${ddlType}_new`]: newLines.length };
}
/**
 * 
 * @param {*} reportDDLFolder 
 * @param {*} srcLines 
 * @param {*} destLines 
 * @param {*} ddlType 
 * @param {*} destEnv 
 * @returns 
 */
async function markDeprecatedDDL(reportDDLFolder, srcLines, destLines, ddlType, destEnv) {
  const deprecatedDDL = `deprecated.list`;
  makeSureFolderExisted(reportDDLFolder);
  saveToFile(reportDDLFolder, deprecatedDDL, '');
  // not in source or OTE_ will be deprecated
  const deprecatedLines = destLines.filter(
    line => line.includes('OTE_') === 0 || !srcLines.includes(line)
  );
  if (deprecatedLines.length > 0) {
    const result = deprecatedLines.sort().join('\n');
    saveToFile(reportDDLFolder, deprecatedDDL, result);
  }
  // add to report
  return { [`${ddlType}_deprecated`]: deprecatedLines.length };
}
/**
 * 
 * @param {*} reportDDLFolder 
 * @param {*} srcLines 
 * @param {*} destLines 
 * @param {*} ddlType 
 * @param {*} srcEnv 
 * @param {*} destEnv 
 * @returns 
 */
async function markChangeDDL(reportDDLFolder, srcLines, destLines, ddlType, srcEnv, destEnv) {
  const updatedDDL = `updated.list`;
  saveToFile(reportDDLFolder, updatedDDL, '');
  makeSureFolderExisted(`reports/diff/${destEnv}/${getDBName(destEnv)}/${ddlType}`);
  emptyDirectory(`reports/diff/${destEnv}/${getDBName(destEnv)}/${ddlType}`);
  // dest
  const existedDDL = srcLines.filter(line => destLines.includes(line));
  makeSureFolderExisted(reportDDLFolder);
  // source
  const currentLines = readFromFile(reportDDLFolder, updatedDDL, 1);
  const updatedLines = existedDDL.filter(findDDLChanged2Migrate(srcEnv, ddlType, destEnv));

  if (updatedLines.length > 0) {
    const result = [...currentLines, ...updatedLines].sort().join('\n');
    saveToFile(reportDDLFolder, updatedDDL, result);
  }
  // add to report
  return { [`${ddlType}_updated`]: updatedLines.length };
}


/**
 * 
 * @param {*} srcEnv 
 * @param {*} ddlType 
 * @param {*} destEnv 
 * @returns 
 */
function findDDLChanged2Migrate(srcEnv, ddlType, destEnv) {
  return ddlName => {
    const srcFolder = `db/${srcEnv}/${getDBName(srcEnv)}/${ddlType}`;
    const destFolder = `db/${destEnv}/${getDBName(destEnv)}/${ddlType}`;
    // 1. get content current ddl ready to compare
    const _domainRegex = /@flo(dev\.net|uat\.net|stage\.com)/g;
    const srcContent = readFromFile(srcFolder, `${ddlName}.sql`)
      .replace(_domainRegex, '@flomail.net');
    const destContent = readFromFile(destFolder, `${ddlName}.sql`)
      .replace(_domainRegex, '@flomail.net');
    if (!srcContent || !destContent) {
      return false;
    }
    if (srcContent !== destContent) {
      // diff content compare
      // vimDiffToHtml(destEnv, ddlType, ddlName, srcContent, destContent, `${ddlName}.sql`);
      return srcContent !== destContent;
    }
  }
}
/**
 * 
 * @param {*} srcEnv 
 * @param {*} ddlType 
 * @param {*} destEnv 
 * @returns 
 */
async function reportDLLChange(srcEnv, ddlType, destEnv = null) {
  try {
    if (!destEnv) {
      destEnv = getDestEnv(srcEnv);
    }
    if (destEnv === srcEnv) {
      return;
    }
    // 1.
    const mapFolder = `map-migrate/${srcEnv}-to-${destEnv}`;
    const reportFolder = `${mapFolder}/${getDBName(srcEnv)}`;
    makeSureFolderExisted(reportFolder);
    // 3. get list current ddl

    //
    const srcContent = readFromFile(`db/${srcEnv}/${getDBName(srcEnv)}/current-ddl`, `${ddlType}.list`);
    const destContent = readFromFile(`db/${destEnv}/${getDBName(destEnv)}/current-ddl`, `${ddlType}.list`);

    if (!srcContent) {
      return;
    }
    // 5. prepare to read all ddl start compare
    const srcLines = srcContent.split('\n').map(line => line.trim()).sort();
    const destLines = destContent.split('\n').map(line => line.trim()).sort();
    // 6. find new ddl
    const newDDL = await markNewDDL(`${reportFolder}/${ddlType}`, srcLines, destLines, ddlType, destEnv);
    // 7. find deprecated ddl
    const deprecatedDDL = await markDeprecatedDDL(`${reportFolder}/${ddlType}`, srcLines, destLines, ddlType, destEnv);
    // 8. compare already existed ddl on 2 ENV
    const updatedDDL = ![FUNCTIONS, PROCEDURES].includes(ddlType)
      ? {}
      : await markChangeDDL(`${reportFolder}/${ddlType}`, srcLines, destLines, ddlType, srcEnv, destEnv);
    //
    await appendReport(destEnv, { ...newDDL, ...deprecatedDDL, ...updatedDDL });
    //
    await report2html(destEnv);
  } catch (error) {
    alog.error(`Error:reportDLLChange:: `, error);
  }
}
/**
 * 
 * @param {*} env 
 */
export const comparator = (ddl) => async (env) => {
  const srcEnv = getSourceEnv(env);
  switch (ddl) {
    case FUNCTIONS:
      await reportDLLChange(srcEnv, FUNCTIONS, env);
      break;
    case PROCEDURES:
      await reportDLLChange(srcEnv, PROCEDURES, env);
      break;
    case TABLES:
      await reportDLLChange(srcEnv, TABLES, env);
      await reportTableStructureChange(env);
      break;
    default:
      report2console(env);
      break;
  }
}