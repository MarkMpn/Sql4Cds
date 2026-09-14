import { ResultSetFilter } from "./protocol";

export async function findInvalidFilter(
  filters: ResultSetFilter[] | undefined,
  probe: (filters: ResultSetFilter[] | undefined) => Promise<boolean>
): Promise<ResultSetFilter | undefined> {
  if (!filters?.length) { return undefined; }

  for (let index = 0; index < filters.length; index++) {
    const remaining = filters.filter((_, candidate) => candidate !== index);
    if (await probe(remaining.length ? remaining : undefined)) {
      return filters[index];
    }
  }

  return undefined;
}