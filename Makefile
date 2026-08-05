build:
	@cargo build

test:
	@cargo nextest run --all-features

# Strict boundary lint: no unwrap/indexing/panic/expect on the lib surfaces
# that handle external input (AGENTS.md § Safety; specs/70). Run on the
# boundary crates only (ingress is the trust boundary; its deps are compiled
# with the same gates).
lint-boundary:
	@for c in consumer_engine-ingress consumer_engine-query consumer_engine-storage consumer_engine-semantic consumer_engine-ingestion; do \
		echo "== $$c =="; \
		cargo clippy -p $$c --lib -- -D warnings -W clippy::unwrap_used -W clippy::indexing_slicing -W clippy::panic -W clippy::expect_used || exit 1; \
	done
	@echo "boundary lint: clean"

# Query-latency GATE (issue #25; docs/research/perf-calibration.md §M1 evidence):
# runs the real-engine bench over B/F/J/P at CE_SCALE_ROWS (default 50000) and
# asserts the LOCKED budgets — P50 < 1s, P99 < 5s (specs/71 §3) — exiting
# non-zero on breach, so the perf budget is an enforced exit criterion, not a
# soft target (CI runners invoke this target directly; no repo CI exists yet).
# Thresholds overridable for testing: CE_MAX_P50_MS / CE_MAX_P99_MS.
bench-queries:
	@cargo run --release -p consumer_engine-query --example query_latency

check-agent-sync:
	@cmp -s CLAUDE.md AGENTS.md || { \
		echo "AGENTS.md must stay in sync with CLAUDE.md"; \
		echo "Update both files with the same shared project instructions."; \
		exit 1; \
	}
	@tmp_dir=$$(mktemp -d); \
	trap 'rm -rf "$$tmp_dir"' EXIT; \
	cp -R .claude/skills "$$tmp_dir/expected-skills"; \
	find "$$tmp_dir/expected-skills" -name SKILL.md -exec perl -0pi -e 's/CLAUDE\.md/AGENTS.md/g; s/Claude/Codex/g; s/claude/codex/g' {} +; \
	diff -ru --exclude agents "$$tmp_dir/expected-skills" .agents/skills || { \
		echo "Codex skills must stay in sync with Claude skills after Claude-to-Codex renaming."; \
		echo "Update .claude/skills first, then mirror the shared content into .agents/skills."; \
		exit 1; \
	}

release:
	@cargo release tag --execute
	@git cliff -o CHANGELOG.md
	@git commit -a -n -m "Update CHANGELOG.md" || true
	@git push origin master
	@cargo release push --execute

update-submodule:
	@git submodule update --init --recursive --remote

.PHONY: build test check-agent-sync release update-submodule lint-boundary bench-queries
