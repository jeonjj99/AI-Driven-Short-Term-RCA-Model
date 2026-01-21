# LLM based Near-Term Forecast RCA, using multiple data sources

## Overview
This project is a near-term (M0–M+1) diagnostic and decision-support model that explains short-horizon business performance using three high-frequency signals:

- **Rank (weekly):** competitiveness / visibility signal  
- **In-Stock (weekly):** availability / execution signal  
- **Sales (weekly POS / USW):** demand signal  

Instead of rebuilding long-range forecasts, the model focuses on **rapid root-cause analysis (RCA)** and **tactical forecast / replenishment adjustments** for the current and next month.

---

## Why this exists
Demand Forecasting team usually spend significant time reviewing exceptions (Under/Over, ranking shifts, availability issues) and translating scattered signals into action. This model reduces review time by:
- Consolidating key signals into a single, consistent narrative
- Producing **multiple plausible scenarios** (not a single rigid conclusion)
- Outputting a structured action plan with explicit evidence references

---

## Key Design Principles
### 1) Short-horizon focus (tactical, not strategic)
- Best suited for **this month / next month** actions
- Avoids overreacting to long-term changes based on short-term noise

### 2) Data-augmented reasoning (RAG-like pattern)
- We “retrieve” the relevant signal slices for a specific Material
- We “augment” them with backend business logic (features/flags)
- The LLM generates an explainable RCA report and recommended actions

### 3) Scenario-based output (reduce rework)
Instead of one definitive answer, the model provides **ranked hypotheses** with supporting evidence, enabling faster human validation and shorter review cycles.

---

## Inputs (Signals)
### Weekly Signals
- **Rank**: competitiveness / visibility proxy (direction inferred or assumed; documented in output)
- **In-Stock %**: availability proxy (OOS risk indicator)
- **POS / USW**: demand proxy (trend inferred from multiple weeks, not single-week spikes), ** USW stands for Unit sales per Store per Weeks, which can represent the real market trend 

---

## Backend Business Logic (Before LLM)
The backend prepares a consistent “business context package” for the LLM. Typical logic includes:

### 1) Time alignment and aggregation
- Weekly POS/USW, Rank, In-Stock preserved at weekly grain
- Monthly Forecast/ASHIP/ESHIP preserved at monthly grain
- Optional week-to-month mapping when needed for reconciliation

### 2) Noise control
- Avoid single-week conclusions
- Use multi-week patterns (e.g., 3–4 week trend) for directionality

### 3) Derived features and flags (examples)
- **Trend slope**: USW/POS trend over last N weeks
- **Instock risk**: count of weeks below threshold, min/median instock
- **Rank shift**: improvement/worsening magnitude and persistence
- **Phase detection**: split into periods when signals change direction

These features reduce ambiguity and help the LLM stay consistent across Materials.

---

## Model Reasoning Framework (LLM Prompt)
The LLM is instructed to:
1) **Observe patterns** across Rank, In-Stock, and POS/USW (over time)
2) Provide a **situation diagnosis** (primary + secondary drivers)
3) List **root causes (ranked)** with **explicit supporting evidence** (exact JSON fields)
4) Produce an **action plan**:
   - Defensive actions (availability / ASN / replenishment)
   - Offensive actions (forecast uplift, expedite, promo support)
   - What to check next (specific missing signals)

### Example decision heuristics (illustrative)
- **Availability-driven loss**: In-Stock down while demand holds → likely lost sales
- **Demand softness**: In-Stock improves but USW/POS declines → likely demand drop
- **Upside capture risk**: Demand rising while In-Stock declines → stockout risk
- **Competitive noise**: Rank moves but demand flat → investigate competitive/exogenous factors

The model can propose new rules if patterns don’t fit predefined scenarios.

---

## Outputs
Each run produces a structured RCA report:

1. **Observed Patterns**
   - Clear direction of Rank / In-Stock / POS/USW over time
2. **Situation Diagnosis**
   - 1–3 drivers with rationale
3. **Root Causes (Ranked) + Evidence**
   - 3–5 causes with exact fields used
4. **Action Plan**
   - Defensive actions
   - Offensive actions
   - What to check next

The output is designed to be copy/paste ready for review decks and decision logs.

---

## How this reduces review time
- **Consistency:** same structure and logic across Materials
- **Speed:** immediate synthesis of multiple signals into decision-ready narratives
- **Fewer back-and-forth cycles:** scenario ranking + evidence makes validation faster
- **Operational relevance:** directly maps findings to replenishment and near-term forecast actions

---

## Intended Use Cases
- Weekly business reviews for exception Materials
- Diagnosing persistent near-term Under/Over behavior
- Identifying availability-driven lost sales vs. true demand softness
- Supporting tactical forecast overrides and shipment prioritization

---

## Limitations (Known)
- Rank is a proxy and can be influenced by factors not present in the data that the company can get
  (e.g., pricing, promotions, category resets, digital visibility changes).
- Since data comes from multiple sources, data integrity should be ensured. Otherwise, worong data can be put into the LLM which can lead to the wrong interpretation
- The model is not intended to replace long-horizon demand planning models.

---

## Roadmap (Optional Enhancements)
- Add event signals: promotion, price index, POG changes, digital visibility metrics. In retail industry, those event can play a crucial role in sales as well as scm sector. 
- Add confidence scoring and “mixed-signal” classification
- Create automated “Next checks” connectors (ASN status, inventory position, lead time,,,)
- Build evaluation set: compare model diagnoses vs planner decisions and outcomes
