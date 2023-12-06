const fs = require('fs');
const path = require('path');

export function readFromFile(folder, fileName, returnArray = false) {
  const filePath = path.join(__basedir, folder, fileName);
  const fileContent = !fs.existsSync(filePath)
    ? ''
    : fs.readFileSync(filePath, 'utf8').trim();
  return !returnArray
    ? fileContent
    : fileContent.split('\n').filter(Boolean)
    ;
}

/**
 * Copy content from source file to destination file, create if not existed
 * @param {string} sourceFile - Path to the source file
 * @param {string} destinationFile - Path to the destination file
 */
export function copyFile(sourceFile, destinationFile) {
  try {
    const content = fs.readFileSync(sourceFile, 'utf8');
    fs.writeFileSync(destinationFile, content);
  } catch (error) {
    // skip error: ENOENT
    if (error.code === 'ENOENT') {
      return;
    }
    console.error('Error occurred while copying the content:', error);
  }
}

/**
 * Save the file
 * @param {*} content
 * @param {*} filePath
 * @returns {Promise<void>}
 */
export function saveToFile(folder, fileName, content) {
  const filePath = path.join(__basedir, folder, fileName);
  if (typeof content !== 'string') {
    const error = new Error(`Invalid content for file ${filePath}`);
    alog.error(error);
    reject(error);
    return;
  }
  if (folder.indexOf(__basedir) == -1) {
    FileManager.getInstance().makeSureFolderExisted(folder);
  }
  fs.writeFileSync(filePath, content);
}


export function makeSureFolderExisted(folder) {
  // const folderPath = path.join(__basedir, folder); // Get the absolute path of the folder

  const folders = folder.split('/');

  // Recursive function to create folders from parent to child
  function createFoldersRecursively(index, currentPath) {
    if (index === folders.length) {
      // Base case: All folders have been created
      return;
    }

    // Append the current folder to the current path
    currentPath = path.join(currentPath, folders[index]);

    // Check if the current folder exists
    if (!fs.existsSync(currentPath)) {
      // If the folder doesn't exist, create it
      fs.mkdirSync(currentPath); // Create the folder synchronously
    }

    // Recursively call the function with the next folder
    createFoldersRecursively(index + 1, currentPath);
  }

  // Start the recursive creation of folders
  createFoldersRecursively(0, __basedir);
}


/**
 * Clean the specified folder by removing all files within it
 * @param {*} folderPath The path to the folder
 */
export function emptyDirectory(folder) {
  try {
    const folderPath = path.join(__basedir, folder);
    if (fs.existsSync(folderPath)) {
      const files = fs.readdirSync(folderPath);
      for (const file of files) {
        const filePath = path.join(folderPath, file);
        fs.unlinkSync(filePath);
      }
    }
  } catch (err) {
    alog.error(`Error cleaning folder ${folder}: `, err);
  }
}
