Read the full prompt document at `docs/NL_to_ISL_Testing_Prompt.md` and execute the complete workflow.

The user wants to generate **$ARGUMENTS** new ISL test commands (default 100 if not specified). Scale the domain distribution proportionally.

Steps:
1. Query the DB for existing `nl_question` values to avoid duplicates
2. Generate the requested number of new ISL test commands distributed by domain area (not ISL feature)
3. Create a test file, run it, fix failures (bugs vs test errors vs design limitations)
4. Document any new design limitations in `docs/NL_Translation_Test_Plan_v2.md`
5. Store validated commands in `isl_prompt_examples` via a new SQL migration
6. Run the full test suite and confirm 0 failures

The prompt file has the complete ISL spec reference, all data sources, RBAC rules, known limitations, and coverage gaps. Read it first before generating any commands.
