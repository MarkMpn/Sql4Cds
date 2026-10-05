export interface DataverseRecordReference {
  dataSource: string;
  logicalName: string;
  id: string;
}

/** Detects serialized Dataverse EntityReference-like values from result cell raw objects. */
export function extractDataverseRecordReference(value: unknown): DataverseRecordReference | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) { return undefined; }
  const candidate = value as Record<string, unknown>;
  const dataSource = stringField(candidate, "dataSource");
  const logicalName = stringField(candidate, "logicalName");
  const id = stringField(candidate, "id");
  if (!dataSource || !logicalName || !id) { return undefined; }
  return { dataSource, logicalName, id };
}

export function buildDataverseRecordUrl(rootUrl: string, reference: DataverseRecordReference): string {
  const base = rootUrl.replace(/\/$/, "");
  return `${base}/main.aspx?etn=${encodeURIComponent(reference.logicalName)}&pagetype=entityrecord&id=${encodeURIComponent(reference.id)}`;
}

function stringField(source: Record<string, unknown>, name: string): string | undefined {
  const value = source[name];
  if (typeof value !== "string") { return undefined; }
  const trimmed = value.trim();
  return trimmed.length ? trimmed : undefined;
}