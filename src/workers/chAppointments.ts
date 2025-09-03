import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { httpGetJson } from "../lib/http.js";
import { parseOfficerName, officerIdFromUrl, monthStr, nameMatches } from "../lib/normalize.js";
import { batchCreate } from "../lib/airtable.js";
import { logger } from "../lib/logger.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";

const WRITE_TO_AIRTABLE = (process.env.WRITE_TO_AIRTABLE || "").toLowerCase() === 'true';

const CH_BASE = process.env.CH_API_BASE || "https://api.company-information.service.gov.uk";
const CH_KEY = process.env.CH_API_KEY || "";

function chHeaders() {
  const auth = Buffer.from(`${CH_KEY}:`).toString("base64");
  return { Authorization: `Basic ${auth}` };
}

type CHOfficer = {
  name: string;
  officer_role: string;
  date_of_birth?: { month: number; year: number };
  links?: { officer?: { appointments?: string } };
};

await initDb();
export default new Worker("ch-appointments", async job => {
  const { companyNumber, firstName, lastName, contactId } = job.data as { companyNumber: string; firstName?: string; lastName?: string; contactId?: string };
  console.log("companyNumber in initDB in workers/chAppointments.ts", companyNumber);
  console.log("firstName in initDB in workers/chAppointments.ts", firstName);
  console.log("lastName in initDB in workers/chAppointments.ts", lastName);
  console.log("contactId in initDB in workers/chAppointments.ts", contactId);

  await startJob({ jobId: job.id as string, queue: 'ch-appointments', name: job.name, payload: job.data });

  try {
    const company = await httpGetJson<any>(`${CH_BASE}/company/${companyNumber}`, { headers: chHeaders() });
    console.log("company in initDB in workers/chAppointments.ts", company);

    await logEvent(job.id as string, 'info', 'Fetched company', { companyNumber, company_name: company.company_name });

    const officers = await httpGetJson<any>(`${CH_BASE}/company/${companyNumber}/officers`, { headers: chHeaders() });
    console.log("officers in initDB in workers/chAppointments.ts", officers);
    
    await logEvent(job.id as string, 'info', 'Fetched officers', { count: (officers.items || []).length });

  // Helper to fetch full appointment list for an officer (with simple pagination)
  async function fetchOfficerAppointments(officerId: string) {
    const items: any[] = [];
    let start = 0;
    const page = 100;
    while (true) {
      const data = await httpGetJson<any>(`${CH_BASE}/officers/${officerId}/appointments?start_index=${start}&items_per_page=${page}`, { headers: chHeaders() });
      const pageItems = (data.items || []) as any[];
      items.push(...pageItems);
      const total = typeof data.total_results === 'number' ? data.total_results : pageItems.length;
      start += pageItems.length;
      if (start >= total || pageItems.length === 0) break;
    }
    return items;
  }

  function joinAddress(addr: any): string {
    if (!addr) return "";
    const parts = [
      addr.address_line_1,
      addr.address_line_2,
      addr.locality,
      addr.region,
      addr.postal_code,
      addr.country,
    ]
      .map((s: any) => (s || "").toString().trim())
      .filter(Boolean);
    return parts.join(", ");
  }

    const peopleRecords: any[] = [];
    const appointmentRecords: any[] = [];

  const officerItems = (officers.items || []) as CHOfficer[];
  // Filter directors and optionally match by provided name
  const directorCandidates = officerItems.filter((o) => {
    if (!o?.name) return false;
    const role = (o.officer_role || "").toLowerCase();
    if (!role.includes("director")) return false;
    if (firstName || lastName) {
      const parsed = parseOfficerName(o.name);
      return nameMatches({ first: firstName, last: lastName }, { first: parsed.first, last: parsed.last });
    }
    return true;
  });

  // If a specific person is requested, take the first good match
  const toProcess = (firstName || lastName) ? directorCandidates.slice(0, 1) : directorCandidates;

  // Cache for company profiles to avoid duplicate calls
  const companyCache = new Map<string, any>();

    for (const o of toProcess) {
    if (!o.name) continue;
    const { first, middle, last } = parseOfficerName(o.name);
    const dob = o.date_of_birth;
    const dobString = dob ? `${monthStr(dob.month)} ${dob.year}` : "";
    const officerUrl = (o.links?.officer?.appointments || "") as string;
    const officerId = officerIdFromUrl(officerUrl) || "";

    // Build appointment list for this officer
    const appts = officerId ? await fetchOfficerAppointments(officerId) : [];
    const enrichedAppointments: any[] = [];
    for (const it of appts) {
      const coNum = it?.appointed_to?.company_number || "";
      const coName = it?.appointed_to?.company_name || "";
      if (!coNum) continue;
      let co = companyCache.get(coNum);
      if (!co) {
        try {
          co = await httpGetJson<any>(`${CH_BASE}/company/${coNum}`, { headers: chHeaders() });
        } catch {
          co = {};
        }
        companyCache.set(coNum, co);
      }
      const address = co?.registered_office_address || {};
      enrichedAppointments.push({
        sic_codes: Array.isArray(co?.sic_codes) ? co.sic_codes : [],
        company_name: coName || co?.company_name || "",
        trading_name: null,
        appointment_id: it?.appointment_id || `${officerId}:${coNum}`,
        company_number: coNum,
        registered_address: joinAddress(address),
        registered_postcode: address?.postal_code || "",
        verified_company_website: [],
        verified_company_linkedIns: [],
        company_website_verification: [],
        company_linkedIn_verification: [],
      });
    }

    peopleRecords.push({
      fields: {
        first_name: first,
        middle_names: middle,
        last_name: last,
        dob_year: dob?.year || null,
        dob_month: dob?.month || null,
        dob_string: dobString,
        officer_ids: [officerId].filter(Boolean),
        status: "Found via CH",
      },
      _enriched: {
        dob_year: dob?.year || null,
        dob_month: dob?.month || null,
        full_name: [first, middle, last].filter(Boolean).join(" "),
        last_name: last,
        contact_id: contactId || null,
        dob_string: dobString,
        first_name: first,
        officer_ids: [officerId].filter(Boolean),
        appointments: enrichedAppointments,
        middle_names: middle || null,
        verified_director_linkedIns: [],
        director_linkedIn_verification: [],
      },
    });

    // Maintain current Appointments table write for traceability
    appointmentRecords.push({
      fields: {
        company_number: companyNumber,
        company_name: company.company_name || "",
        officer_role: o.officer_role || "",
        officer_id: officerId,
      },
    });
    }

  // Also check PSCs if no directorial match was found
    if (!peopleRecords.length) {
    try {
      const pscs: any = await httpGetJson<any>(`${CH_BASE}/company/${companyNumber}/persons-with-significant-control`, { headers: chHeaders() });
      const pscItems = (pscs.items || []) as any[];
      for (const p of pscItems) {
        const nameRaw: string | undefined =
          (p as any).name ||
          (((p as any).name_elements?.forename && (p as any).name_elements?.surname)
            ? `${(p as any).name_elements.surname}, ${(p as any).name_elements.forename}`
            : undefined);
        if (!nameRaw) continue;
        const parsed = parseOfficerName(nameRaw);
        if (firstName || lastName) {
          const match = nameMatches({ first: firstName, last: lastName }, { first: parsed.first, last: parsed.last });
          if (!match) continue;
        }
        peopleRecords.push({
          fields: {
            first_name: parsed.first,
            middle_names: parsed.middle,
            last_name: parsed.last,
            status: "Found via CH PSC",
          },
          _enriched: {
            dob_year: null,
            dob_month: null,
            full_name: [parsed.first, parsed.middle, parsed.last].filter(Boolean).join(" "),
            last_name: parsed.last,
            contact_id: contactId || null,
            dob_string: "",
            first_name: parsed.first,
            officer_ids: [],
            appointments: [],
            middle_names: parsed.middle || null,
            verified_director_linkedIns: [],
            director_linkedIn_verification: [],
          },
        });
      }
    } catch (e) {
      // PSC endpoint may not exist or may be restricted; ignore if it fails
    }
    }

  // Persist minimal records to Airtable as before
    // Persist minimal records to Airtable only after we have full enriched results
    if (WRITE_TO_AIRTABLE) {
      if (peopleRecords.length) await batchCreate("People", peopleRecords.map(({ fields }) => ({ fields })));
      if (appointmentRecords.length) await batchCreate("Appointments", appointmentRecords);
    } else {
      await logEvent(job.id as string, 'info', 'Airtable write skipped', {
        reason: 'WRITE_TO_AIRTABLE=false',
        tables: ['People', 'Appointments'],
        people: peopleRecords.length,
        appointments: appointmentRecords.length
      });
    }

    const enriched = peopleRecords.map((r) => r._enriched);
    await completeJob(job.id as string, { companyNumber, matches: enriched.length, enriched });
    logger.info({ companyNumber, matches: enriched.length }, "CH processed");
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ companyNumber, err: String(err) }, 'CH worker failed');
    throw err;
  }
}, { connection, concurrency: 2 });
