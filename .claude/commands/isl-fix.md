Read the full prompt document at `docs/ISL_Design_Limitations_Fix_Prompt.md` and execute **Stage 1** only.

The user wants to analyze **$ARGUMENTS** ISL design limitations (default: all open limitations).

- If specific numbers like `3,5` are provided, only analyze those limitation numbers
- If `all` or no argument is provided, analyze ALL open & fixable limitations

**This is the ANALYSIS stage.** Do NOT implement any code changes yet. Your job is to:

1. Read `docs/NL_Translation_Test_Plan_v2.md` — discover and categorize all limitations
2. Read ALL ISL source files (validator, compiler, schema, normalizer, prompt) in full
3. Read existing test files to understand test patterns
4. For each open limitation: trace through the pipeline, find the exact root cause (function, line, condition)
5. Determine the next round number by checking `docs/fix-plans/` for existing files (round-1.md, round-2.md, etc.)
6. Generate `docs/fix-plans/round-N.md` with EXACT code changes — paste real current code and real replacement code
7. Include complete test code ready to copy-paste
8. Order fixes by risk (schema → validator → compiler → new syntax)

The output is `docs/fix-plans/round-N.md` — a specific, hardcoded fix plan that can be reviewed before execution.

After this completes, the user should review the generated round file and then run `/project:isl-fix-run` to execute the fixes.
