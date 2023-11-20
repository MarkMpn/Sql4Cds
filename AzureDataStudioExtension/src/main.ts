/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the Source EULA. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
'use strict';

import * as vscode from 'vscode';
import * as path from 'path';
import { SqlOpsDataClient, ClientOptions } from 'dataprotocol-client';
import { IConfig, ServerProvider, Events } from '@microsoft/ads-service-downloader';
import { LogMessageNotification, LogMessageParams, MessageType, PublishDiagnosticsParams, RevealOutputChannelOn, ServerOptions, TransportKind } from 'vscode-languageclient';

import * as Constants from './constants';
import * as Utils from './utils';
import { LanguageClientErrorHandler } from './errorHandler';

const baseConfig = require('./config.json');
const outputChannel = vscode.window.createOutputChannel(Constants.serviceName);
const statusView = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left);

class ProgressNotication {
	msg: string;
	progress: number;
}

export async function activate(context: vscode.ExtensionContext) {

	// lets make sure we support this platform first
	let supported = await Utils.verifyPlatform();

	if (!supported) {
		vscode.window.showErrorMessage('Unsupported platform');
		return;
	}

	let config: IConfig = JSON.parse(JSON.stringify(baseConfig));
	config.installDirectory = path.join(__dirname, config.installDirectory);
	config.proxy = vscode.workspace.getConfiguration('http').get('proxy');
	config.strictSSL = vscode.workspace.getConfiguration('http').get('proxyStrictSSL') || true;

	let languageClient: SqlOpsDataClient;
	const serverdownloader = new ServerProvider(config);

	serverdownloader.eventEmitter.onAny(generateHandleServerProviderEvent());

	let clientOptions: ClientOptions = {
		providerId: Constants.providerId,
		errorHandler: new LanguageClientErrorHandler(),
		documentSelector: ['sql'],
		synchronize: {
			configurationSection: Constants.providerId
		},
		outputChannelName: Constants.providerId,
		revealOutputChannelOn: RevealOutputChannelOn.Info
	};

	let diagnosticCollection = vscode.languages.createDiagnosticCollection(Constants.providerId);
	context.subscriptions.push(diagnosticCollection);

	let e = path.join(Utils.getResolvedServiceInstallationPath(), "MarkMpn.Sql4Cds.LanguageServer.dll");
	let serverOptions = generateServerOptions(e);
	languageClient = new SqlOpsDataClient(Constants.serviceName, serverOptions, clientOptions);
	languageClient.onReady().then(() => {
		statusView.text = Constants.serviceName + ' Started';
		setTimeout(() => {
			statusView.hide();
		}, 1500);
		languageClient.onNotification("sql4cds/progress", (msg: string) => {
			statusView.text = msg;
			statusView.show();
		});
		languageClient.onNotification("sql4cds/confirmation", (message: {ownerUri: string, msg: string}) => {
			vscode.window
				.showInformationMessage(message.msg, "Yes", "All", "No")
				.then(answer => {
					languageClient.sendNotification("sql4cds/confirm", { ownerUri: message.ownerUri, result: answer })
				});
		});
		languageClient.onNotification("query/batchComplete", () => {
			statusView.hide();
		});
		languageClient.onNotification("window/logMessage", (message: LogMessageParams) => {
			switch (message.type) {
				case MessageType.Error:
					languageClient.error(message.message); break;

				case MessageType.Warning:
					languageClient.warn(message.message); break;

				case MessageType.Info:
					languageClient.info(message.message); break;

				default:
					languageClient.outputChannel.appendLine(message.message); break;
			}
		});
		languageClient.onNotification("textDocument/publishDiagnostics", (message: PublishDiagnosticsParams) => {
			var diagnostics = message.diagnostics
			.map((d) => new vscode.Diagnostic(
				new vscode.Range(
					new vscode.Position(
						d.range.start.line,
						d.range.start.character
					),
					new vscode.Position(
						d.range.end.line,
						d.range.end.character
					)
				),
				d.message,
				d.severity
				));
			diagnosticCollection.set(vscode.Uri.parse(message.uri), diagnostics);
		});
	});
	statusView.show();
	statusView.text = 'Starting ' + Constants.serviceName;
	languageClient.start();

	context.subscriptions.push({ dispose: () => languageClient.stop() });
}

function generateServerOptions(executablePath: string): ServerOptions {
	let serverArgs = [executablePath];

	let config = vscode.workspace.getConfiguration(Constants.providerId);
	if (config) {
		// Override the server path with the local debug path if enabled

		let useLocalSource = config["useDebugSource"];
		if (useLocalSource) {
			let localSourcePath = config["debugSourcePath"];
			let filePath = path.join(localSourcePath, "MarkMpn.Sql4Cds.LanguageServer.dll");
			serverArgs[0] = filePath;

			let enableStartupDebugging = config["enableStartupDebugging"];
			if (enableStartupDebugging)
				serverArgs.push('--enable-remote-debugging-wait');
		}

		let logFileLocation = path.join(Utils.getDefaultLogLocation(), Constants.providerId);
		serverArgs.push('--log-dir=' + logFileLocation);

		// Enable diagnostic logging in the service if it is configured
		let logDebugInfo = config["logDebugInfo"];
		if (logDebugInfo) {
			serverArgs.push('--enable-logging');
		}
	}

	// run the service host
	return { command: "dotnet", args: serverArgs, transport: TransportKind.stdio };
}

function generateHandleServerProviderEvent() {
	let dots = 0;
	return (e: string, ...args: any[]) => {
		outputChannel.show();
		statusView.show();
		switch (e) {
			case Events.INSTALL_START:
				outputChannel.appendLine(`Installing ${Constants.serviceName} to ${args[0]}`);
				statusView.text = 'Installing Service';
				break;
			case Events.INSTALL_END:
				outputChannel.appendLine('Installed');
				break;
			case Events.DOWNLOAD_START:
				outputChannel.appendLine(`Downloading ${args[0]}`);
				outputChannel.append(`(${Math.ceil(args[1] / 1024)} KB)`);
				statusView.text = 'Downloading Service';
				break;
			case Events.DOWNLOAD_PROGRESS:
				let newDots = Math.ceil(args[0] / 5);
				if (newDots > dots) {
					outputChannel.append('.'.repeat(newDots - dots));
					dots = newDots;
				}
				break;
			case Events.DOWNLOAD_END:
				outputChannel.appendLine('Done!');
				break;
		}
	};
}

// this method is called when your extension is deactivated
export function deactivate(): void {
}
