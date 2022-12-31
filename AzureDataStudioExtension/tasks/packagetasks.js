var gulp = require('gulp');
var fs = require('fs');
var cproc = require('child_process');
var del = require('del');
var path = require('path');

async function installService() {
    const targetPath = path.join(__dirname, '../out/' + 'sql4cdstoolsservice');
    const srcPath = path.join(__dirname, '../../MarkMpn.Sql4Cds.LanguageServer/bin/Debug/net7.0');

    return new Promise(function(resolve, reject) {
        gulp
            .src([srcPath + '/**/*'])
            .pipe(gulp.dest(targetPath))
            .on('error', reject)
            .on('end', resolve)
    });
}

gulp.task('ext:install-service', () => {
    return installService();
});

function doPackageSync(packageName) {
    const vsceArgs = [];
    vsceArgs.push('vsce');
    vsceArgs.push('package'); // package command
    vsceArgs.push('--yarn'); // to use yarn list instead on npm list

    if (packageName !== undefined) {
        vsceArgs.push('-o');
        vsceArgs.push(packageName);
    }
    const command = vsceArgs.join(' ');
    console.log(command);
    return cproc.execSync(command);
}

function cleanServiceInstallFolder() {
    return new Promise((resolve, reject) => {
        const root = path.join(__dirname, '../out/' + 'sql4cdstoolsservice');
        console.log('Deleting Service Install folder: ' + root);
        del(root + '/*').then(() => {
            resolve();
        }).catch((error) => {
            reject(error)
        });
    });
}

function doOfflinePackage(packageName) {
    return installService().then(() => {
       return doPackageSync(packageName + '.vsix');
    });
}

//Install vsce to be able to run this task: npm install -g vsce
gulp.task('package:offline', () => {
    const json = JSON.parse(fs.readFileSync('package.json'));
    const name = json.name;
    const version = json.version;
    const packageName = name + '-' + version;

    let promise = Promise.resolve();
    cleanServiceInstallFolder().then(() => {
        promise = promise.then(() => {
            return doOfflinePackage(packageName).then(() => {
                return cleanServiceInstallFolder();
            });
        });
    });

    return promise;
});
