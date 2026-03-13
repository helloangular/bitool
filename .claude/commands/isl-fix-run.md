Read the full prompt document at `docs/ISL_Design_Limitations_Fix_Prompt.md` and execute **Stage 2** only.

**Prerequisites:** A fix plan must exist in `docs/fix-plans/`. Find the LATEST `round-N.md` file with status "PENDING".

If **$ARGUMENTS** specifies a round number (e.g., `2`), use `docs/fix-plans/round-2.md` specifically. Otherwise, use the latest pending round file.

Read the fix plan and execute each fix mechanically, in the priority order specified in the plan.

For each fix in the plan:
1. Apply the EXACT code change specified (old code → new code)
2. Add the test code from the plan to the appropriate test file
3. Run the specific test: `lein test bny.isl.[test-namespace]`
4. Run the full suite: `lein test` — must be 0 failures, 0 regressions
5. If a test fails, diagnose and adjust — do NOT move to the next fix until current is green

After all fixes pass:
1. Update `docs/NL_Translation_Test_Plan_v2.md` — mark limitations as FIXED with strikethrough
2. Update `docs/NL_to_ISL_Testing_Prompt.md` — update Known Design Limitations
3. Update `src/clj/bny/isl/nl/prompt.clj` if LLM guidance needs to change
4. Run final `lein test` — confirm 0 failures
5. Update the round file status to: `> **Status:** APPLIED on [date] — all N fixes implemented, M tests added, 0 regressions`
