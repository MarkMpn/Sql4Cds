'use strict';
import * as vscode from 'vscode';
import { ErrorAction, ErrorHandler, Message, CloseAction } from 'vscode-languageclient';
import * as Constants from './constants';
import * as opener from 'opener';

/**
 * Handle Language Service client errors
 * @class LanguageClientErrorHandler
 */
export class LanguageClientErrorHandler implements ErrorHandler {

	/**
	 * Show an error message prompt with a link to known issues wiki page
	 * @memberOf LanguageClientErrorHandler
	 */
	showOnErrorPrompt(): void {
		vscode.window.showErrorMessage(
			Constants.serviceCrashMessage,
			Constants.serviceCrashButton).then(action => {
				if (action && action === Constants.serviceCrashButton) {
					opener(Constants.serviceCrashLink);
				}
			});
	}

	/**
	 * Callback for language service client error
	 *
	 * @param {Error} error
	 * @param {Message} message
	 * @param {number} count
	 * @returns {ErrorAction}
	 *
	 * @memberOf LanguageClientErrorHandler
	 */
	error(error: Error, message: Message, count: number): ErrorAction {
		this.showOnErrorPrompt();

		// we don't retry running the service since crashes leave the extension
		// in a bad, unrecovered state
		return ErrorAction.Shutdown;
	}

	/**
	 * Callback for language service client closed
	 *
	 * @returns {CloseAction}
	 *
	 * @memberOf LanguageClientErrorHandler
	 */
	closed(): CloseAction {
		this.showOnErrorPrompt();

		// we don't retry running the service since crashes leave the extension
		// in a bad, unrecovered state
		return CloseAction.DoNotRestart;
	}
}