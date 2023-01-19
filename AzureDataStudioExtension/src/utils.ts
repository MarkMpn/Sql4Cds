/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the Source EULA. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
'use strict';

import * as path from 'path';
import * as crypto from 'crypto';
import * as os from 'os';

var baseConfig = require('./config.json');

// The function is a duplicate of \src\paths.js. IT would be better to import path.js but it doesn't
// work for now because the extension is running in different process.
export function getAppDataPath() {
	let platform = process.platform;
	switch (platform) {
		case 'win32': return process.env['APPDATA'] || path.join(process.env['USERPROFILE'], 'AppData', 'Roaming');
		case 'darwin': return path.join(os.homedir(), 'Library', 'Application Support');
		case 'linux': return process.env['XDG_CONFIG_HOME'] || path.join(os.homedir(), '.config');
		default: throw new Error('Platform not supported');
	}
}

export function getDefaultLogLocation() {
	return path.join(getAppDataPath(), 'sqlops');
}

export function ensure(target: object, key: string): any {
	if (target[key] === void 0) {
		target[key] = {} as any;
	}
	return target[key];
}

export interface IPackageInfo {
	name: string;
	version: string;
	aiKey: string;
}

export function getPackageInfo(packageJson: any): IPackageInfo {
	if (packageJson) {
		return {
			name: packageJson.name,
			version: packageJson.version,
			aiKey: packageJson.aiKey
		};
	}
}

export function verifyPlatform(): Thenable<boolean> {
	if (os.platform() === 'darwin' && parseFloat(os.release()) < 16.0) {
		return Promise.resolve(false);
	} else {
		return Promise.resolve(true);
	}
}

export function getServiceInstallConfig() {
    let config = JSON.parse(JSON.stringify(baseConfig));
    config.installDirectory = path.join(__dirname, config.installDirectory);

    return config;
}

export function getResolvedServiceInstallationPath(): string{
	let config = getServiceInstallConfig();
	let dir = config.installDirectory;

	return dir;
}
