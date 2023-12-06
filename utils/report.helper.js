const fs = require('fs');
const path = require('path');
const { saveToFile, makeSureFolderExisted, readFromFile } = require('./file.helper');
const { getSourceEnv, getDBName,
  STATUSES: { NEW, UPDATED, DEPRECATED },
  DDL: { TABLES, PROCEDURES, FUNCTIONS }
} = require('../configs/db');


const { spawn } = require('child_process');

/**
 *   Usage example
     const file1 = 'file1.txt';
     const file2 = 'file2.txt';
     const outputFilename = 'output.html';
    convertToHtml(file1, file2, outputFilename);
 */

export const vimDiffToHtml = (destEnv, ddlType, ddlName, folder1, folder2, fname) => {
  makeSureFolderExisted(`reports/diff/${destEnv}/${getDBName(destEnv)}/${ddlType}`);
  //
  const reportPath = `reports/diff/${destEnv}/${getDBName(destEnv)}/${ddlType}`;
  const outputFilename = path.join(reportPath, `${ddlName}.html`);
  const vimProcess = spawn('vimdiff', [
    '-c', 'TOhtml',
    `wq! ${outputFilename}`,
    '-c', 'qa!',
    `${folder1}/${fname}`, `${folder2}/${fname}`
  ]);

  vimProcess.on('error', (error) => {
    alog.error(`Error: ${error.message}`);
  });

  vimProcess.stderr.on('data', (data) => {
    alog.error(`stderr: ${data}`);
  });

  vimProcess.on('close', (code) => {
    if (code === 0) {
      alog.info(`HTML file generated: ${outputFilename}`);
      vimProcess.stdin.end();
    } else {
      alog.error(`Vimdiff process exited with code ${code}`);
    }
  });
};

export const appendReport = async (env, newReport) => {
  try {
    const folder = `reports/json`;
    const fileName = `${getDBName(env)}.${env}.json`;
    // 1. read
    const oldReport = readFromFile(folder, fileName);
    // 2. array to object
    const reports = revertAndJoin(oldReport);
    // 3. focus missing
    const newKey = Object.keys(newReport)[0] || '';
    if (Object.keys(reports).includes(newKey)
      && newKey === 'columns_missing') {
      newReport = {
        [newKey]: !newReport[newKey]
          ? {}
          : {
            ...reports[newKey],
            ...newReport[newKey]
          }
      }
    }
    // 4. object to array
    const finalReport = Object.keys(reports)
      .sort()
      .reduce((obj, key) => {
        obj[key] = reports[key];
        return obj;
      }, {});
    const splitConvert = splitAndConvert({ ...finalReport, ...newReport });
    // 5. append & write
    saveToFile(folder, fileName, JSON.stringify(splitConvert, null, 2));
  } catch (error) {
    alog.error(`Error:reportDiff:: `, error);
  }
}

export const report2console = (destEnv) => {
  const report = readFromFile(`reports/json`, `${getDBName(destEnv)}.${destEnv}.json`);
  alog.info(`\n████▓▓▓▓▒▒▒▒░░░░ REPORT: ${getSourceEnv(destEnv)} to ${destEnv} ░░░░▒▒▒▒▓▓▓▓████
  ${report.replace(/[{}",\[\]]/g, '')}
  ████▓▓▓▓▒▒▒▒░░░░ REPORT: ${getSourceEnv(destEnv)} to ${destEnv} ░░░░▒▒▒▒▓▓▓▓████`);
}

export const report2html = async (destEnv) => {
  // 1. read template
  const template = readFromFile('reports/html', 'template.html');
  // 2. read report
  const report = readFromFile('reports/json', `${getDBName(destEnv)}.${destEnv}.json`);
  const rr = revertAndJoin(report);
  // order by F P T
  const TOTAL_DDL = JSON.stringify([
    rr.functions_total || 0,
    rr.procedures_total || 0,
    rr.tables_total || 0,
    '-',
    '-'
  ]);
  //
  const NEW_DDL = JSON.stringify([
    rr.functions_new || 0,
    rr.procedures_new || 0,
    rr.tables_new || 0,
    '-',
    '-'
  ]);
  //
  const UPDATED_DDL = JSON.stringify([
    rr.functions_updated || 0,
    rr.procedures_updated || 0,
    rr.tables_updated || 0,
    rr.indexes_updated || 0,
    rr.columns_updated || 0,
  ]);
  //
  const DEPRECATED_DDL = JSON.stringify([
    rr.functions_deprecated || 0,
    rr.procedures_deprecated || 0,
    rr.tables_deprecated || 0,
    '-',
    '-'
  ]);
  const srcEnv = getSourceEnv(destEnv);

  const folderMap/*             */ = `/map-migrate/${srcEnv}-to-${destEnv}/${getDBName(srcEnv)}`;

  const tablePath/*             */ = `${folderMap}/${TABLES}`;
  const TABLE_NEW /*            */ = genDDLItem(tablePath, NEW);
  const TABLE_UPDATE /*         */ = genDDLItem(tablePath, UPDATED);
  const TABLE_DEPRECATED /*     */ = genDDLItem(tablePath, DEPRECATED);

  const procedurePath/*         */ = `${folderMap}/${PROCEDURES}`;
  const PROCEDURE_NEW/*         */ = genDDLItem(procedurePath, NEW);
  const PROCEDURE_DEPRECATED/*  */ = genDDLItem(procedurePath, DEPRECATED);
  const PROCEDURE_UPDATE/*      */ = genDDLItem(procedurePath, UPDATED, PROCEDURES, destEnv);

  const functionPath/*          */ = `${folderMap}/${FUNCTIONS}`;
  const FUNCTION_NEW /*         */ = genDDLItem(functionPath, NEW);
  const FUNCTION_DEPRECATED /*  */ = genDDLItem(functionPath, DEPRECATED);
  const FUNCTION_UPDATE /*      */ = genDDLItem(functionPath, UPDATED, FUNCTIONS, destEnv);

  //
  const MISSING_COLUMNS = JSON.stringify(rr.columns_missing || '{}', null, 2).replace(/\"\,\{\}/g, '');
  // 3. write down report
  const reportHTML = template
    .replace(/{{ENV}}/g, `${getSourceEnv(destEnv)} with ${destEnv}`)
    .replace(/{{TOTAL_DDL}}/, TOTAL_DDL)
    .replace(/{{NEW_DDL}}/, NEW_DDL)
    .replace(/{{UPDATED_DDL}}/, UPDATED_DDL)
    .replace(/{{DEPRECATED_DDL}}/, DEPRECATED_DDL)
    .replace(/{{MISSING_COLUMNS}}/, MISSING_COLUMNS)
    .replace(/{{STYLE4MISSING}}/, MISSING_COLUMNS === '{}' ? 'display:none' : '')
    .replace(/{{TABLE_NEW}}/, TABLE_NEW)
    .replace(/{{TABLE_UPDATE}}/, TABLE_UPDATE)
    .replace(/{{TABLE_DEPRECATED}}/, TABLE_DEPRECATED)
    .replace(/{{PROCEDURE_NEW}}/, PROCEDURE_NEW)
    .replace(/{{PROCEDURE_UPDATE}}/, PROCEDURE_UPDATE)
    .replace(/{{PROCEDURE_DEPRECATED}}/, PROCEDURE_DEPRECATED)
    .replace(/{{FUNCTION_NEW}}/, FUNCTION_NEW)
    .replace(/{{FUNCTION_UPDATE}}/, FUNCTION_UPDATE)
    .replace(/{{FUNCTION_DEPRECATED}}/, FUNCTION_DEPRECATED)

  saveToFile(`reports`, `${getDBName(destEnv)}.${destEnv}.html`, reportHTML);
}

function genDDLItem(ddlPath, status, ddlType, destEnv = null) {
  const ddlList = readFromFile(ddlPath, `${status}.list`)?.split('\n') || [];
  if (!ddlList.length) {
    return `<li>None</li>`;
  }
  if (destEnv) {
    return ddlList.map((ddl) => (`<li><a href="/reports/diff/${destEnv}/${getDBName(destEnv)}/${ddlType}/${ddl}.html">${ddl}</a></li>`)).join('\n');

  }
  return ddlList.map((ddl) => (`<li>${ddl}</li>`)).join('\n');
}
/** 
 * Input: 
 * const inputObj = { 
 *   'parent1-child1': 'value1', 
 *   'parent1-child2': 'value2', 
 *   'parent2-child3': 'value3', 
 *   'parent2-child4': 'value4', 
 *   'prop1': 'value5' 
 * }; 
 *  
 * const separator = '-'; 
 *  
 * Output: 
 * { 
 *   parent1: { 
 *     child1: 'value1', 
 *     child2: 'value2' 
 *   }, 
 *   parent2: { 
 *     child3: 'value3', 
 *     child4: 'value4' 
 *   }, 
 *   prop1: 'value5' 
 * } 
 */
function splitAndConvert(inputObj, separator = '_') {
  const resultObj = {};
  for (const propName in inputObj) {
    if (inputObj.hasOwnProperty(propName)) {
      const propParts = propName.split(separator);
      if (propParts.length === 2) {
        const parentKey = propParts[0];
        const childKey = propParts[1];
        if (!resultObj[parentKey]) {
          resultObj[parentKey] = {};
        }
        resultObj[parentKey][childKey] = inputObj[propName];
      } else {
        resultObj[propName] = inputObj[propName];
      }
    }
  }
  return resultObj;
}
/** 
 * Input: 
 * const inputObj = { 
 *   parent1: { 
 *     child1: 'value1', 
 *     child2: 'value2' 
 *   }, 
 *   parent2: { 
 *     child3: 'value3', 
 *     child4: 'value4' 
 *   } 
 * }; 
 *  
 * const separator = '-'; 
 *  
 * Output: 
 * { 
 *   'parent1-child1': 'value1', 
 *   'parent1-child2': 'value2', 
 *   'parent2-child3': 'value3', 
 *   'parent2-child4': 'value4' 
 * } 
 */
function revertAndJoin(inputObj, separator = '_') {
  if ('string' === typeof inputObj) {
    if (inputObj.length === '{}'.length) { return {}; }
    try {
      inputObj = JSON.parse(inputObj);
    } catch (error) {
      inputObj = {};
    }
  }
  const resultObj = {};
  for (const parentKey in inputObj) {
    if (!inputObj.hasOwnProperty(parentKey)) {
      resultObj[parentKey] = inputObj[parentKey];
      continue;
    }
    const childObj = inputObj[parentKey];
    for (const childKey in childObj) {
      if (!childObj.hasOwnProperty(childKey)) {
        continue;
      }
      const propName = `${parentKey}${separator}${childKey}`;
      resultObj[propName] = childObj[childKey];
    }
  }

  return resultObj;
}