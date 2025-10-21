import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { companyQ } from "../queues/index.js";
import { chGetJson } from "../lib/companiesHouseClient.js";
import { parseOfficerName, officerIdFromUrl, monthStr, nameMatches, normalizeWord } from "../lib/normalize.js";
import { batchCreate } from "../lib/airtable.js";
import { logger } from "../lib/logger.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { query } from "../lib/db.js";

const WRITE_TO_AIRTABLE = (process.env.WRITE_TO_AIRTABLE || "").toLowerCase() === 'true';

type CHOfficer = {
  name: string;
  officer_role: string;
  date_of_birth?: { month: number; year: number };
  links?: { officer?: { appointments?: string } };
};

await initDb();
export default new Worker("ch-appointments", async job => {
  const { companyNumber, firstName, lastName, contactId, rootJobId } = job.data as { companyNumber: string; firstName?: string; lastName?: string; contactId?: string; rootJobId?: string };
  const pipelineRootId = rootJobId || (job.id as string);
  // console.log("companyNumber in initDB in workers/chAppointments.ts", companyNumber);
  // console.log("firstName in initDB in workers/chAppointments.ts", firstName);
  // console.log("lastName in initDB in workers/chAppointments.ts", lastName);
  // console.log("contactId in initDB in workers/chAppointments.ts", contactId);

  await startJob({ jobId: job.id as string, queue: 'ch-appointments', name: job.name, payload: job.data });

  try {
      const company = await chGetJson<any>(`/company/${companyNumber}`);
      // console.log("company in initDB in workers/chAppointments.ts", company);

      await logEvent(job.id as string, 'info', 'Fetched company', { companyNumber, company_name: company.company_name, rootJobId: pipelineRootId });

      const officers = await chGetJson<any>(`/company/${companyNumber}/officers`);
      // console.log("officers in initDB in workers/chAppointments.ts", officers);
      
      await logEvent(job.id as string, 'info', 'Fetched officers', {
        count: (officers.items || []).length,
        officers: (Array.isArray(officers.items) ? officers.items : []).map((o: any) => ({
          name: o?.name || '',
          role: o?.officer_role || '',
          dob: o?.date_of_birth || null,
          links: o?.links || null
        }))
      });

    // Helper to fetch appointment list for an officer (with simple pagination)
    // Optionally limit the total number of items fetched to reduce load when used for filtering.
    async function fetchOfficerAppointments(officerId: string, maxTotal?: number) {
      const items: any[] = [];
      let start = 0;
      const page = 100;
      while (true) {
        const data = await chGetJson<any>(
          `/officers/${officerId}/appointments?start_index=${start}&items_per_page=${page}`
        );
        const pageItems = (data.items || []) as any[];
        items.push(...pageItems);
        const total = typeof data.total_results === 'number' ? data.total_results : pageItems.length;
        start += pageItems.length;
        if ((typeof maxTotal === 'number' && items.length >= maxTotal) || start >= total || pageItems.length === 0) break;
      }
      return (typeof maxTotal === 'number') ? items.slice(0, maxTotal) : items;
    }

    // Search officers by name via CH search API
    async function searchOfficersByName(queryStr: string) {
      if (!queryStr.trim()) return [] as any[];
      try {
        const res = await chGetJson<any>(
          `/search/officers?q=${encodeURIComponent(queryStr)}&items_per_page=50`
        );
        return Array.isArray(res.items) ? res.items : [];
      } catch {
        return [] as any[];
      }
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
    console.log("toProcess in initDB in workers/chAppointments.ts", toProcess);
    
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
        console.log("appts in initDB in workers/chAppointments.ts", appts);

        const enrichedAppointments: any[] = [];
        // Attempt to capture nationality from the officer object or appointment sample (optional field)
        const nationality: string | null = (o as any)?.nationality
          ? String((o as any).nationality).trim()
          : (appts?.[0]?.nationality || appts?.[0]?.person?.nationality || appts?.[0]?.officer?.nationality
              ? String(appts?.[0]?.nationality || appts?.[0]?.person?.nationality || appts?.[0]?.officer?.nationality).trim()
              : null);
        for (const it of appts) {
          const coNum = it?.appointed_to?.company_number || "";
          const coName = it?.appointed_to?.company_name || "";
          if (!coNum) continue;
          let co = companyCache.get(coNum);
          if (!co) {
            try {
              co = await chGetJson<any>(`/company/${coNum}`);
            } catch {
              co = {};
            }
            companyCache.set(coNum, co);
          }
          const address = co?.registered_office_address || {};
          enrichedAppointments.push({
            sic_codes: Array.isArray(co?.sic_codes) ? co.sic_codes : [],
            company_name: coName || co?.company_name || "",
            company_status: it?.appointed_to?.company_status || co?.company_status || "",
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
        console.log("enrichedAppointments in initDB in workers/chAppointments.ts length", enrichedAppointments.length);
        console.log("enrichedAppointments in initDB in workers/chAppointments.ts", enrichedAppointments);

        let extraOfficerIds: string[] = [];
        if ((firstName || lastName) && (first || last)) {
          
          const baselineDobMonth = dob?.month || null;
          const baselineDobYear = dob?.year || null;
          
          const baselineNationality: string | null = (o as any)?.nationality ? String((o as any).nationality).trim() : null;

          const queries = new Set<string>();
          const q1 = [first, last].filter(Boolean).join(" ");
          if (q1) queries.add(q1);
          if (middle) {
            const q2 = [first, middle, last].filter(Boolean).join(" ");
            if (q2) queries.add(q2);
          }

          const searchResults: any[] = [];
          for (const q of queries) {
            const items = await searchOfficersByName(q);
            searchResults.push(...items);
          }

          console.log("searchResults in initDB in workers/chAppointments.ts", searchResults);

          // Helper to get officerId and parse name/DOB from search item
          function parseSearchItem(item: any) {
            const nameRaw = (item?.title || item?.name || "") as string;
            const nm = parseOfficerName(nameRaw);
            const links = item?.links || {};
            // CH search results commonly expose the appointments URL at links.self
            const apptUrl = links?.officer?.appointments || links?.appointments || links?.self || "";
            const id = officerIdFromUrl(apptUrl) || null;
            const dobObj = item?.date_of_birth || item?.items?.date_of_birth || null;
            const dobMonth = typeof dobObj?.month === 'number' ? dobObj.month : null;
            const dobYear = typeof dobObj?.year === 'number' ? dobObj.year : null;
            return { id, name: nm, dobMonth, dobYear };
          }

          // Exact name match requirement for first/last
          function exactFirstLastMatch(a: { first: string; last: string }, b: { first: string; last: string }) {
            return normalizeWord(a.first) === normalizeWord(b.first) && normalizeWord(a.last) === normalizeWord(b.last);
          }

          // Middle name match if baseline has one
          // Allow: exact match OR candidate missing OR candidate initial matches baseline
          function middleNameAcceptable(baseMiddle: string, candMiddle: string) {
            const bm = normalizeWord(baseMiddle);
            if (!bm) return true; // nothing to check
            const cm = normalizeWord(candMiddle);
            if (!cm) return true; // candidate omitted middle name
            if (bm === cm) return true;
            return bm[0] && cm[0] && bm[0] === cm[0];
          }

          // Filter and collect distinct officer IDs that appear to be the same person
          const relatedOfficerIds = new Set<string>();
          for (const item of searchResults) {
            const parsed = parseSearchItem(item);
            if (!parsed.id) continue;
            // Skip if it's the same officer id we already processed
            if (parsed.id === officerId) continue;
            // Name exact match (first & last)
            if (!exactFirstLastMatch({ first, last }, { first: parsed.name.first, last: parsed.name.last })) continue;
            // Middle name check
            if (!middleNameAcceptable(middle, parsed.name.middle)) continue;
            // DOB month/year must be exact if baseline specifies
            if (baselineDobMonth) {
              if (!parsed.dobMonth || parsed.dobMonth !== baselineDobMonth) continue;
            }
            if (baselineDobYear) {
              if (!parsed.dobYear || parsed.dobYear !== baselineDobYear) continue;
            }

            // Nationality: only enforce if baseline exists AND candidate provides a value
            if (baselineNationality) {
              const sample = await fetchOfficerAppointments(parsed.id, 1);
              const nat = (sample[0]?.nationality || sample[0]?.person?.nationality || sample[0]?.officer?.nationality || "").toString();
              const cn = normalizeWord(nat);
              const bn = normalizeWord(baselineNationality);
              if (cn && cn !== bn) continue;
            }
            relatedOfficerIds.add(parsed.id);
          }

          console.log("relatedOfficerIds in initDB in workers/chAppointments.ts", relatedOfficerIds);

          if (relatedOfficerIds.size) {
            await logEvent(job.id as string, 'info', 'Found related officer records', { count: relatedOfficerIds.size });
          }

          // For each related officer id, fetch and merge their appointments
          for (const relId of relatedOfficerIds) {
            const relAppts = await fetchOfficerAppointments(relId);
            for (const it of relAppts) {
              const coNum = it?.appointed_to?.company_number || "";
              const coName = it?.appointed_to?.company_name || "";
              if (!coNum) continue;
              let co = companyCache.get(coNum);
              if (!co) {
                try {
                  co = await chGetJson<any>(`/company/${coNum}`);
                } catch {
                  co = {};
                }
                companyCache.set(coNum, co);
              }
              const address = co?.registered_office_address || {};
              enrichedAppointments.push({
                sic_codes: Array.isArray(co?.sic_codes) ? co.sic_codes : [],
                company_name: coName || co?.company_name || "",
                company_status: it?.appointed_to?.company_status || co?.company_status || "",
                trading_name: null,
                appointment_id: it?.appointment_id || `${relId}:${coNum}`,
                company_number: coNum,
                registered_address: joinAddress(address),
                registered_postcode: address?.postal_code || "",
                verified_company_website: [],
                verified_company_linkedIns: [],
                company_website_verification: [],
                company_linkedIn_verification: [],
              });
            }
          }
          console.log("enrichedAppointments after related officers in initDB in workers/chAppointments.ts length", enrichedAppointments.length);
          console.log("enrichedAppointments after related officers in initDB in workers/chAppointments.ts", enrichedAppointments);

          // Dedupe appointments by appointment_id
          const seen = new Set<string>();
          const deduped: any[] = [];
          for (const e of enrichedAppointments) {
            const id = String(e.appointment_id || "");
            if (!id || seen.has(id)) continue;
            seen.add(id);
            deduped.push(e);
          }
          enrichedAppointments.length = 0;
          enrichedAppointments.push(...deduped);
          extraOfficerIds = Array.from(relatedOfficerIds);
        }

        peopleRecords.push({
          fields: {
            first_name: first,
            middle_names: middle,
            last_name: last,
            dob_year: dob?.year || null,
            dob_month: dob?.month || null,
            dob_string: dobString,
            officer_ids: [officerId, ...extraOfficerIds].filter(Boolean),
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
            nationality: nationality || null,
            officer_ids: [officerId, ...extraOfficerIds].filter(Boolean),
            appointments: enrichedAppointments,
            middle_names: middle || null,
            verified_director_linkedIns: [],
            director_linkedIn_verification: [],
          },
        });

        console.log("peopleRecords in initDB in workers/chAppointments.ts", peopleRecords);

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
        const pscs: any = await chGetJson<any>(
          `/company/${companyNumber}/persons-with-significant-control`
        );
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
          console.log("peopleRecords from PSC in initDB in workers/chAppointments.ts", peopleRecords);
        }
      } catch (e) {
        // PSC endpoint may not exist or may be restricted; ignore if it fails
      }
    }

    // Persist minimal records to Airtable as before
    // Persist minimal records to Airtable only after we have full enriched results
    console.log("peopleRecords before Database commit/Airtable write in initDB in workers/chAppointments.ts length", peopleRecords.length);
    console.log("peopleRecords before Database commit/Airtable write in initDB in workers/chAppointments.ts", peopleRecords);
    console.log("peopleRecords[0].peopleRecords[0]?._enriched?.appointments.length before Database commit/Airtable write in initDB in workers/chAppointments.ts", peopleRecords[0]?._enriched?.appointments.length);
    console.log("peopleRecords[0]?._enriched?.appointments before Database commit/Airtable write in initDB in workers/chAppointments.ts", peopleRecords[0]?._enriched?.appointments);

    if (WRITE_TO_AIRTABLE) {
      if (peopleRecords.length) await batchCreate("People", peopleRecords.map(({ fields }) => ({ fields })));
      if (appointmentRecords.length) await batchCreate("Appointments", appointmentRecords);
    } else {
      // await logEvent(job.id as string, 'info', 'Airtable write skipped', {
      //   reason: 'WRITE_TO_AIRTABLE=false',
      //   tables: ['People', 'Appointments'],
      //   people: peopleRecords.length,
      //   appointments: appointmentRecords.length
      // });
    }
    
// Persist enriched results to Postgres for next-stage processing
    let pgPeopleWritten = 0;
    let pgAppointmentsWritten = 0;
    try {
      let totalAppts = 0;
      for (const rec of peopleRecords) {
        const e = rec._enriched || {};
        const f = rec.fields || {};
        const personKey = [
          normalizeWord(e.first_name || f.first_name || ''),
          normalizeWord(e.middle_names || f.middle_names || ''),
          normalizeWord(e.last_name || f.last_name || ''),
          String(e.dob_year || f.dob_year || ''),
          String(e.dob_month || f.dob_month || '')
        ].join('|');
        const officerIds = Array.isArray(e.officer_ids) ? e.officer_ids : [];
        const { rows: personRows } = await query<{ id: number }>(
          `INSERT INTO ch_people(job_id, root_job_id, person_key, contact_id, first_name, middle_names, last_name, full_name, dob_month, dob_year, dob_string, officer_ids, nationality, status, discovery_job_ids, sitefetch_job_ids, person_linkedin_job_ids)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
           ON CONFLICT (job_id, person_key)
           DO UPDATE SET contact_id=EXCLUDED.contact_id,
                         officer_ids=EXCLUDED.officer_ids,
                         nationality=EXCLUDED.nationality,
                         status=EXCLUDED.status,
                         root_job_id=EXCLUDED.root_job_id,
                         discovery_job_ids=ARRAY(SELECT DISTINCT UNNEST(COALESCE(ch_people.discovery_job_ids,'{}') || EXCLUDED.discovery_job_ids))::text[],
                         sitefetch_job_ids=ARRAY(SELECT DISTINCT UNNEST(COALESCE(ch_people.sitefetch_job_ids,'{}') || EXCLUDED.sitefetch_job_ids))::text[],
                         person_linkedin_job_ids=ARRAY(SELECT DISTINCT UNNEST(COALESCE(ch_people.person_linkedin_job_ids,'{}') || EXCLUDED.person_linkedin_job_ids))::text[],
                         updated_at=now()
           RETURNING id`,
          [
            job.id as string,
            pipelineRootId,
            personKey,
            e.contact_id || null,
            e.first_name || null,
            e.middle_names || null,
            e.last_name || null,
            e.full_name || null,
            e.dob_month || null,
            e.dob_year || null,
            e.dob_string || null,
            officerIds.length ? officerIds : null,
            e.nationality || null,
            f.status || 'Found via CH',
            [job.id as string],
            [],
            []
          ]
        );
        const personId = personRows[0]?.id;
        if (personId && Array.isArray(e.appointments)) {
          for (const a of e.appointments) {
            totalAppts++;
            await query(
              `INSERT INTO ch_appointments(person_id, appointment_id, company_number, company_name, company_status, registered_address, registered_postcode, sic_codes,
                                           verified_company_website, verified_company_linkedIns, company_website_verification, company_linkedIn_verification)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
               ON CONFLICT (person_id, appointment_id)
               DO UPDATE SET company_number=EXCLUDED.company_number,
                             company_name=EXCLUDED.company_name,
                             company_status=EXCLUDED.company_status,
                             registered_address=EXCLUDED.registered_address,
                             registered_postcode=EXCLUDED.registered_postcode,
                             sic_codes=EXCLUDED.sic_codes,
                             verified_company_website=EXCLUDED.verified_company_website,
                             verified_company_linkedIns=EXCLUDED.verified_company_linkedIns,
                             company_website_verification=EXCLUDED.company_website_verification,
                             company_linkedIn_verification=EXCLUDED.company_linkedIn_verification,
                             updated_at=now()`,
              [
                personId,
                a.appointment_id || null,
                a.company_number || null,
                a.company_name || null,
                a.company_status || null,
                a.registered_address || null,
                a.registered_postcode || null,
                Array.isArray(a.sic_codes) ? a.sic_codes : null,
                JSON.stringify(a.verified_company_website || []),
                JSON.stringify(a.verified_company_linkedIns || []),
                JSON.stringify(a.company_website_verification || []),
                JSON.stringify(a.company_linkedIn_verification || [])
              ]
            );
          }
        }
      }
      pgPeopleWritten = peopleRecords.length;
      pgAppointmentsWritten = totalAppts;
      await logEvent(job.id as string, 'info', 'Persisted CH results to Postgres', { people: peopleRecords.length, appointments: totalAppts });
      try {
        // Expanded details for development visibility
        const enrichedDetail = peopleRecords.map(r => r._enriched);
        await logEvent(job.id as string, 'debug', 'CH results detail', { people: enrichedDetail.length, enriched: enrichedDetail });
      } catch {}
    } catch (e) {
      await logEvent(job.id as string, 'error', 'Failed to persist CH results to Postgres', { error: String(e) });
    }
    const enriched = peopleRecords.map((r) => r._enriched);

    // Build a unique company list from all appointments and prioritise ACTIVE
    type CoItem = { company_number: string; company_name: string; company_status: string; registered_address?: string; registered_postcode?: string };
    const coMap = new Map<string, CoItem>();
    for (const e of enriched) {
      const apps: any[] = Array.isArray(e?.appointments) ? e.appointments : [];
      for (const a of apps) {
        const num = (a?.company_number || "").toString();
        if (!num) continue;
        const status = (a?.company_status || "").toString().toLowerCase();
        const item: CoItem = {
          company_number: num,
          company_name: (a?.company_name || "").toString(),
          company_status: status,
          registered_address: (a?.registered_address || "").toString(),
          registered_postcode: (a?.registered_postcode || "").toString(),
        };
        const existing = coMap.get(num);
        // Prefer ACTIVE over any existing status
        if (!existing || (existing.company_status !== 'active' && item.company_status === 'active')) {
          coMap.set(num, item);
        }
      }
    }

    const allCompanies = Array.from(coMap.values());
    const activeCompanies = allCompanies.filter(c => c.company_status === 'active');
    const laterCompanies = allCompanies.filter(c => c.company_status !== 'active');

    // Enqueue discovery jobs: ACTIVE immediately at higher priority, others later with delay
    let enqActive = 0, enqLater = 0;
    for (const c of activeCompanies) {
      const jobId = `co:${job.id}:${c.company_number}`;
      await companyQ.add(
        "discover",
        { companyNumber: c.company_number, companyName: c.company_name, address: c.registered_address, postcode: c.registered_postcode, rootJobId: pipelineRootId },
        { jobId, priority: 1, attempts: 5, backoff: { type: "exponential", delay: 1500 } }
      );
      try {
        await query(
          `UPDATE ch_people
              SET discovery_job_ids = (
                    SELECT ARRAY(SELECT DISTINCT UNNEST(COALESCE(ch_people.discovery_job_ids,'{}') || ARRAY[$1::text]))
               ),
                  updated_at = now()
            WHERE job_id = $2`,
          [jobId, job.id as string]
        );
      } catch (e) {
        await logEvent(job.id as string, 'warn', 'Failed to tag discovery job on people', { jobId, error: String(e) });
      }
      enqActive++;
    }
    // Skip discovery for non-active (e.g., dissolved) companies
    await logEvent(job.id as string, 'info', 'Enqueued company-discovery follow-ups (active only)', { active: enqActive, skipped_non_active: laterCompanies.length });

    await completeJob(job.id as string, {
      companyNumber,
      matches: enriched.length,
      enriched,
      pg: { people: pgPeopleWritten, appointments: pgAppointmentsWritten },
      enqueued: {
        active: enqActive,
        later: enqLater,
        companies: activeCompanies.map(c => c.company_number)
      }
    });
    logger.info({ companyNumber, matches: enriched.length, pg: { people: pgPeopleWritten, appointments: pgAppointmentsWritten }, enqueued: { active: enqActive, skipped_non_active: laterCompanies.length, companies: activeCompanies.map(c => c.company_number) } }, "CH processed");
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ companyNumber, err: String(err) }, 'CH worker failed');
  }
}, { connection, concurrency: 2 });
