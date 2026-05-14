# Circul demo access — for WORK team

**Live site:** https://circul.polsia.app

> ✅ **Status:** Send-ready. Driver feature shipped to prod 2026-05-12 (PR #89). All 7 personas + cross-tier transaction chain seeded on prod 2026-05-14 via migration `seed_work_demo_personas`. Single coherent demo with driver in-scope from day one.

Seven operator-role logins below, one per tier of the supply chain plus the driver who moves material between aggregator and processor. Best experienced in cross-tier order: collector → aggregator → driver → processor → recycler → converter, with agent shown as the field-ops role under aggregator.

## What you'll see

Circul models a 5-tier supply chain — Collector → Aggregator → Processor → Recycler → Converter — plus Agent as a field-ops role attached to aggregators. Each role has its own dashboard, and the same transaction shows up at multiple tiers as it moves through the chain. The "cross-tier story" is the easiest way to feel the product:

1. **Log in as Naa Adjeley Lamptey (Collector)** → see drop-offs you've made to Quansah Recovery, your earnings, your rating.
2. **Log in as Quansah Recovery (Aggregator)** → see the same drop-offs from the buying side, plus pending transactions awaiting confirmation, P&L, and dispatches to Sankofa Plastics.
3. **Log in as Yaa Boateng (Agent)** → see the field-ops view of Quansah Recovery's yard activity.
4. **Log in as Selorm Agbeko (Driver)** → see dispatches assigned to you, confirm pickup-from-aggregator and arrival-at-processor events on USSD.
5. **Log in as Sankofa Plastics (Processor)** → see deliveries received from aggregators including Quansah, with confirmation events from Selorm, sourcing reports, sales to Veolia.
6. **Log in as Veolia (Recycler)** → see processed flake received, sales to Alpla.
7. **Log in as Alpla Group (Converter)** → see recycled pellets received, end of the chain.

Same transaction, different vantage points. That's the core value of the data model.

## Login credentials

### Phone + PIN (operator roles using USSD-first flow)

| Role | Name | Phone | PIN |
|---|---|---|---|
| Collector | Naa Adjeley Lamptey | `0241555001` | `4321` |
| Aggregator | Quansah Recovery (Kwesi Quansah) | `0241555002` | `5342` |
| Agent (under Aggregator) | Yaa Boateng | `0241555003` | `6453` |
| Driver (under Aggregator) | Selorm Agbeko | `0241555004` | `7546` |

Phone-based roles also have a web dashboard at https://circul.polsia.app/ using the same phone + PIN.

### Email + password (back-office roles using web-first flow)

| Role | Entity | Email | Password |
|---|---|---|---|
| Processor | Sankofa Plastics | `sankofa@circul.demo` | `WorkDemo2026!` |
| Recycler | Veolia | `veolia@circul.demo` | `WorkDemo2026!` |
| Converter | Alpla Group | `alpla@circul.demo` | `WorkDemo2026!` |

## USSD demo (optional — Ghana SIM required)

If anyone on the WORK team has access to a Ghana SIM, they can dial `*920*54#` to experience the USSD flows on a feature phone — that's how collectors and aggregators in the field actually use Circul.

If a Ghana SIM isn't available, the web dashboards mirror the same data — you'll see the outputs of USSD activity even without dialing.

## What shipped since 2026-05-06

Both features locked in our 2026-05-06 conversation are now live on prod:

- **Driver actor** — shipped 2026-05-12 (PR #89). Drivers are a standalone platform actor with phone+PIN auth, able to confirm pickup-from-aggregator and arrival-at-processor events via USSD. Selorm Agbeko (login below) is wired into Quansah Recovery's dispatches.
- **Light Impact Partner dashboard** — shipped earlier than the 2026-05-20 target. A dedicated read-only view scoped to actors WORK tags, with monthly tonnage reports exportable as PDF and Excel. Vivien has a dedicated IP login (separate from the seven WORK-team operator logins below) — she'll receive that directly.

## Notes

- This is demo state. Data is illustrative, not production.
- If you hit any rough edges, please flag them. The 2026-05-06 mid-demo USSD crash root cause was identified as Africa's Talking's per-step user-response timeout (30-60s depending on MNO) — every USSD step now fits inside that envelope.
- Questions to Jojo directly.

---

## Pre-send checklist for Jojo

**Gate cleared 2026-05-14.** Driver shipped (2026-05-12) and demo data seeded on prod (migration `seed_work_demo_personas`, 2026-05-14 18:40 UTC).

Verified automatically:

- [x] **Driver feature shipped to prod.** PR #89 merged 2026-05-12. Smoke 35/41, lint 0err/35warn preserved.
- [x] **All 7 accounts created on prod.** Confirmed by Polsia query 2026-05-14: Naa id=124, Quansah id=18, Yaa id=2, Selorm id=1, Sankofa id=7, Veolia id=2, Alpla id=6.
- [x] **Demo data seeded.** Cross-tier chain populated and verified on prod: Naa→Quansah 3/35.5kg, Quansah→Sankofa 2/450kg (driver=Selorm), Sankofa→Veolia 2/370kg, Veolia→Alpla 1/200kg. Driver-link active, Vivien IP tag confirmed.
- [x] **No real PII in demo data.** Seed migration only writes the 7 named demo personas; no real-user names or phone numbers.

Manual smoke before sending:

- [ ] **All 7 accounts log in successfully** at https://circul.polsia.app/. (Five-minute click-through — try each row in the credential tables above.)
- [ ] **Front page looks clean.** Visit https://circul.polsia.app/ logged-out as a sanity check.
- [ ] **Agent works in two contexts.** Yaa's `0241555003 / 6453` works on web. (USSD path is optional — same conditional as below.)
- [ ] **USSD smoke (conditional on Ghana SIM access).** If a Ghana SIM is in reach: confirm `0241555004 / 7546` (Selorm) can complete a pickup-confirmation flow and `0241555001 / 4321` (Naa Adjeley) can dial `*920*54#`. If no Ghana SIM is available, ship with a note that web dashboards mirror the USSD-side data.

If any item fails, fix before sending — WORK saw the USSD crash mid-demo and is calibrated to notice rough edges.
