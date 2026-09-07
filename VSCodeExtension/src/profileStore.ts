import * as crypto from "node:crypto";
import * as vscode from "vscode";
import { AuthenticationType, ConnectionDetails, ConnectionProfile } from "./protocol";

const profilesKey = "sql4cds.connectionProfiles";
const redirectUrl = "https://login.microsoftonline.com/common/oauth2/nativeclient";
const secretNames = ["password", "clientSecret", "connectionString"] as const;
type SecretName = typeof secretNames[number];

export interface ProfileChangeEvent {
  readonly type: "added" | "updated" | "removed";
  readonly profile: ConnectionProfile;
  readonly previous?: ConnectionProfile;
  readonly credentialsChanged?: boolean;
}

interface ProfileDraft {
  readonly profile: ConnectionProfile;
  readonly secrets: Partial<Record<SecretName, string>>;
}

export class ProfileStore implements vscode.Disposable {
  private readonly changedEmitter = new vscode.EventEmitter<ProfileChangeEvent>();
  private readonly revisions = new Map<string, number>();
  public readonly onDidChange = this.changedEmitter.event;

  constructor(private readonly context: vscode.ExtensionContext) {}

  public get profiles(): readonly ConnectionProfile[] {
    return this.context.globalState.get<ConnectionProfile[]>(profilesKey, []);
  }

  public get(id: string): ConnectionProfile | undefined {
    return this.profiles.find(profile => profile.id === id);
  }

  public revision(id: string): number { return this.revisions.get(id) ?? 0; }

  public async addInteractive(): Promise<ConnectionProfile | undefined> {
    const id = crypto.randomUUID();
    const draft = await this.collectProfile(id);
    if (!draft) { return undefined; }

    await this.persist(draft, undefined);
    this.touch(id);
    this.changedEmitter.fire({ type: "added", profile: draft.profile });
    return draft.profile;
  }

  public async editInteractive(id: string): Promise<ConnectionProfile | undefined> {
    const existing = this.get(id);
    if (!existing) {
      void vscode.window.showErrorMessage("That SQL 4 CDS connection no longer exists.");
      return undefined;
    }

    const draft = await this.collectProfile(id, existing);
    if (!draft) { return undefined; }

    await this.persist(draft, existing);
    this.touch(id);
    this.changedEmitter.fire({
      type: "updated",
      profile: draft.profile,
      previous: existing,
      credentialsChanged: Object.keys(draft.secrets).length > 0
    });
    return draft.profile;
  }

  public async renameInteractive(id: string): Promise<ConnectionProfile | undefined> {
    const existing = this.get(id);
    if (!existing) {
      void vscode.window.showErrorMessage("That SQL 4 CDS connection no longer exists.");
      return undefined;
    }

    const name = await vscode.window.showInputBox({
      title: "Rename SQL 4 CDS connection",
      prompt: "Connection name",
      value: existing.name,
      ignoreFocusOut: true,
      validateInput: value => this.validateName(value, existing.id)
    });
    if (name === undefined) { return undefined; }

    const profile = { ...existing, name: name.trim() };
    await this.context.globalState.update(profilesKey, this.profiles.map(item => item.id === id ? profile : item));
    this.touch(id);
    this.changedEmitter.fire({ type: "updated", profile, previous: existing });
    return profile;
  }

  public async remove(id: string): Promise<void> {
    const profile = this.get(id);
    if (!profile) { return; }
    const answer = await vscode.window.showWarningMessage(`Remove SQL 4 CDS connection '${profile.name}'?`, { modal: true }, "Remove");
    if (answer !== "Remove") { return; }

    const remaining = this.profiles.filter(item => item.id !== id);
    await this.context.globalState.update(profilesKey, remaining);
    await Promise.all(secretNames.map(key => this.context.secrets.delete(this.secretKey(id, key))));
    await this.updateHasProfilesContext(remaining.length);
    this.touch(id);
    this.changedEmitter.fire({ type: "removed", profile });
  }

  public async pick(placeHolder = "Select a SQL 4 CDS connection", excludeId?: string): Promise<ConnectionProfile | undefined> {
    const profiles = this.profiles.filter(profile => profile.id !== excludeId);
    if (profiles.length === 0) {
      void vscode.window.showInformationMessage(excludeId
        ? "Add another SQL 4 CDS connection before switching."
        : "Add a SQL 4 CDS connection first.");
      return undefined;
    }
    const selection = await vscode.window.showQuickPick(
      profiles.map(profile => ({ label: profile.name, description: profile.url ?? "Connection string", profile })),
      { placeHolder, ignoreFocusOut: true }
    );
    return selection?.profile;
  }

  public async toConnectionDetails(profile: ConnectionProfile): Promise<ConnectionDetails> {
    const current = this.get(profile.id);
    if (!current) { throw new Error(`The saved connection '${profile.name}' no longer exists.`); }
    profile = current;

    const validationError = validateProfile(profile);
    if (validationError) { throw new Error(validationError); }

    const options: Record<string, unknown> = { connectionName: profile.name, connectionId: profile.id };
    if (profile.authenticationType === "ConnectionString") {
      options.connectionString = await this.requiredSecret(profile, "connectionString");
      return { options };
    }
    options.authenticationType = profile.authenticationType;
    options.url = profile.url;
    if (profile.user) { options.user = profile.user; }
    if (profile.authenticationType === "SqlLogin") {
      options.password = await this.requiredSecret(profile, "password");
    } else if (profile.authenticationType === "None") {
      options.clientid = profile.clientId;
      options.clientsecret = await this.requiredSecret(profile, "clientSecret");
      options.redirectUrl = profile.redirectUrl ?? redirectUrl;
    }
    return { options };
  }

  private async collectProfile(id: string, existing?: ConnectionProfile): Promise<ProfileDraft | undefined> {
    const name = await vscode.window.showInputBox({
      title: existing ? "Edit SQL 4 CDS connection" : "Add SQL 4 CDS connection",
      prompt: "Connection name",
      value: existing?.name,
      ignoreFocusOut: true,
      validateInput: value => this.validateName(value, existing?.id)
    });
    if (name === undefined) { return undefined; }

    const authItems: (vscode.QuickPickItem & { value: AuthenticationType })[] = [
      { label: "Microsoft Entra interactive", description: "Dataverse online", value: "AzureMFA" },
      { label: "Client secret", description: "Application user / server-to-server", value: "None" },
      { label: "Username and password", description: "Internet-facing deployment", value: "SqlLogin" },
      { label: "Windows integrated", description: "On-premises", value: "Integrated" },
      { label: "Connection string", description: "Advanced", value: "ConnectionString" }
    ];
    const auth = await vscode.window.showQuickPick(authItems.map(item => ({ ...item, picked: item.value === existing?.authenticationType })), {
      title: "Authentication type",
      ignoreFocusOut: true,
      placeHolder: existing ? "Choose the authentication type (the current type is selected)" : undefined
    });
    if (!auth) { return undefined; }

    const profile: ConnectionProfile = { id, name: name.trim(), authenticationType: auth.value };
    const secrets: Partial<Record<SecretName, string>> = {};
    const selectedSecret = secretForAuth(auth.value);
    const savedSecret = existing && existing.authenticationType === auth.value && selectedSecret
      ? await this.context.secrets.get(this.secretKey(id, selectedSecret))
      : undefined;
    const keepsExistingSecret = (secret: SecretName): boolean => selectedSecret === secret && Boolean(savedSecret);

    if (auth.value === "ConnectionString") {
      const value = await vscode.window.showInputBox({
        title: "Dataverse connection string",
        prompt: keepsExistingSecret("connectionString") ? "Leave blank to keep the saved connection string" : undefined,
        password: true,
        ignoreFocusOut: true,
        validateInput: input => {
          if (!input && keepsExistingSecret("connectionString")) { return undefined; }
          return validateConnectionString(input);
        }
      });
      if (value === undefined) { return undefined; }
      if (value) { secrets.connectionString = value.trim(); }
    } else {
      const url = await vscode.window.showInputBox({
        title: "Dataverse URL",
        prompt: "https://org.crm.dynamics.com",
        value: existing?.authenticationType === auth.value ? existing?.url : undefined,
        ignoreFocusOut: true,
        validateInput: input => validateDataverseUrl(input)
      });
      if (url === undefined) { return undefined; }
      profile.url = normalizeDataverseUrl(url);

      if (auth.value === "AzureMFA") {
        const user = await vscode.window.showInputBox({
          title: "User name (optional)",
          prompt: "Used to preselect the interactive account",
          value: existing?.authenticationType === auth.value ? existing?.user : undefined,
          ignoreFocusOut: true
        });
        if (user === undefined) { return undefined; }
        profile.user = user.trim() || undefined;
      } else if (auth.value === "SqlLogin") {
        const user = await vscode.window.showInputBox({
          title: "User name",
          value: existing?.authenticationType === auth.value ? existing?.user : undefined,
          ignoreFocusOut: true,
          validateInput: required("Enter the user name.")
        });
        if (user === undefined) { return undefined; }
        profile.user = user.trim();
        const password = await this.collectSecret("Password", "password", keepsExistingSecret("password"));
        if (password === undefined) { return undefined; }
        if (password) { secrets.password = password; }
      } else if (auth.value === "None") {
        const clientId = await vscode.window.showInputBox({
          title: "Application (client) ID",
          value: existing?.authenticationType === auth.value ? existing?.clientId : undefined,
          ignoreFocusOut: true,
          validateInput: validateClientId
        });
        if (clientId === undefined) { return undefined; }
        profile.clientId = clientId.trim();
        const secret = await this.collectSecret("Client secret", "clientSecret", keepsExistingSecret("clientSecret"));
        if (secret === undefined) { return undefined; }
        if (secret) { secrets.clientSecret = secret; }
        profile.redirectUrl = redirectUrl;
      }
    }

    return { profile, secrets };
  }

  private async collectSecret(title: string, name: SecretName, keepExisting: boolean): Promise<string | undefined> {
    return vscode.window.showInputBox({
      title,
      prompt: keepExisting ? `Leave blank to keep the saved ${title.toLocaleLowerCase()}` : undefined,
      password: true,
      ignoreFocusOut: true,
      validateInput: value => !value && !keepExisting ? `Enter the ${title.toLocaleLowerCase()}.` : undefined
    });
  }

  private async persist(draft: ProfileDraft, existing: ConnectionProfile | undefined): Promise<void> {
    const previousSecret = existing ? secretForAuth(existing.authenticationType) : undefined;
    const nextSecret = secretForAuth(draft.profile.authenticationType);
    const profiles = existing
      ? this.profiles.map(item => item.id === draft.profile.id ? draft.profile : item)
      : [...this.profiles, draft.profile];
    const changedSecrets = Object.entries(draft.secrets) as [SecretName, string][];
    const priorValues = new Map<SecretName, string | undefined>();
    try {
      for (const [name, value] of changedSecrets) {
        priorValues.set(name, await this.context.secrets.get(this.secretKey(draft.profile.id, name)));
        await this.context.secrets.store(this.secretKey(draft.profile.id, name), value);
      }
      await this.context.globalState.update(profilesKey, profiles);
    } catch (error) {
      await Promise.all([...priorValues].map(([name, value]) => value === undefined
        ? this.context.secrets.delete(this.secretKey(draft.profile.id, name))
        : this.context.secrets.store(this.secretKey(draft.profile.id, name), value)));
      throw error;
    }
    if (previousSecret && previousSecret !== nextSecret) {
      await this.context.secrets.delete(this.secretKey(draft.profile.id, previousSecret));
    }
    await this.updateHasProfilesContext(profiles.length);
  }

  private validateName(value: string, currentId?: string): string | undefined {
    const name = value.trim();
    if (!name) { return "Enter a connection name."; }
    if (this.profiles.some(profile => profile.id !== currentId && profile.name.localeCompare(name, undefined, { sensitivity: "accent" }) === 0)) {
      return "A connection with this name already exists.";
    }
    return undefined;
  }

  private async requiredSecret(profile: ConnectionProfile, name: SecretName): Promise<string> {
    const value = await this.context.secrets.get(this.secretKey(profile.id, name));
    if (!value) { throw new Error(`The saved credentials for '${profile.name}' are missing. Edit the connection and enter them again.`); }
    return value;
  }

  private async updateHasProfilesContext(count: number): Promise<void> {
    await vscode.commands.executeCommand("setContext", "sql4cds.hasProfiles", count > 0);
  }

  private touch(id: string): void { this.revisions.set(id, this.revision(id) + 1); }

  private secretKey(id: string, name: SecretName): string { return `sql4cds.profile.${id}.${name}`; }

  public dispose(): void { this.changedEmitter.dispose(); }
}

export function normalizeDataverseUrl(value: string): string {
  const candidate = value.trim();
  const parsed = new URL(candidate.includes("://") ? candidate : `https://${candidate}`);
  parsed.pathname = parsed.pathname === "/" ? "" : parsed.pathname.replace(/\/$/, "");
  return parsed.toString().replace(/\/$/, "");
}

export function validateDataverseUrl(value: string): string | undefined {
  if (!value.trim()) { return "Enter the Dataverse URL."; }
  try {
    const parsed = new URL(value.includes("://") ? value.trim() : `https://${value.trim()}`);
    if (parsed.protocol !== "https:") { return "The Dataverse URL must use HTTPS."; }
    if (!parsed.hostname) { return "Enter a valid Dataverse URL."; }
    if (parsed.username || parsed.password) { return "Do not include credentials in the Dataverse URL."; }
    if (parsed.search || parsed.hash) { return "Remove query parameters and fragments from the Dataverse URL."; }
    return undefined;
  } catch {
    return "Enter a valid Dataverse URL, for example https://org.crm.dynamics.com.";
  }
}

export function validateProfile(profile: ConnectionProfile): string | undefined {
  if (!profile.name.trim()) { return "The connection name is missing."; }
  if (profile.authenticationType === "ConnectionString") { return undefined; }
  const urlError = validateDataverseUrl(profile.url ?? "");
  if (urlError) { return urlError; }
  if (profile.authenticationType === "SqlLogin" && !profile.user?.trim()) { return "The user name is missing."; }
  if (profile.authenticationType === "None") { return validateClientId(profile.clientId ?? ""); }
  return undefined;
}

function validateConnectionString(value: string): string | undefined {
  if (!value.trim()) { return "Enter the Dataverse connection string."; }
  const url = /(?:^|;)\s*(?:Service\s*Uri|ServiceUri|Url|Server)\s*=\s*([^;]+)/i.exec(value)?.[1]?.trim().replace(/^["']|["']$/g, "");
  if (!url) { return "The connection string must include Url, ServiceUri, or Server."; }
  return validateDataverseUrl(url);
}

function validateClientId(value: string): string | undefined {
  if (!value.trim()) { return "Enter the application (client) ID."; }
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value.trim())) {
    return "Enter a valid application (client) ID.";
  }
  return undefined;
}

function required(message: string): (value: string) => string | undefined {
  return value => value.trim() ? undefined : message;
}

function secretForAuth(auth: AuthenticationType): SecretName | undefined {
  if (auth === "ConnectionString") { return "connectionString"; }
  if (auth === "SqlLogin") { return "password"; }
  if (auth === "None") { return "clientSecret"; }
  return undefined;
}
