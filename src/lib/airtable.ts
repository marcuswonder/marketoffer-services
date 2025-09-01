import Airtable from "airtable";

const { AIRTABLE_API_KEY, AIRTABLE_BASE_ID } = process.env;
if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID) {
  throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
}
export const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

export async function batchCreate(table: string, records: any[]) {
  // Chunk into 10s per Airtable API limits
  const chunks: any[] = [];
  for (let i=0;i<records.length;i+=10) chunks.push(records.slice(i, i+10));
  for (const chunk of chunks) {
    await base(table).create(chunk, { typecast: true });
  }
}

export async function batchUpdate(table: string, updates: any[]) {
  const chunks: any[] = [];
  for (let i=0;i<updates.length;i+=10) chunks.push(updates.slice(i, i+10));
  for (const chunk of chunks) {
    await base(table).update(chunk, { typecast: true });
  }
}

export const tables = {
  People: (selectOpts: any = {}) => base("People").select(selectOpts).all(),
  Companies: (selectOpts: any = {}) => base("Companies").select(selectOpts).all(),
  Appointments: (selectOpts: any = {}) => base("Appointments").select(selectOpts).all(),
  Jobs: (selectOpts: any = {}) => base("Jobs").select(selectOpts).all()
};
